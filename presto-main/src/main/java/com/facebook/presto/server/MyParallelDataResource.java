/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.facebook.presto.Session;
import com.facebook.presto.client.ParallelDataResults;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.stats.TimeStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_NEXT_URI;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/paralleldata")
public class MyParallelDataResource
{
    private static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(2, SECONDS);
    private static final DataSize MAX_RESULT_SIZE = new DataSize(1, DataSize.Unit.MEGABYTE);

    private final TaskManager taskManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final TimeStat readFromOutputBufferTime = new TimeStat();
    private final TimeStat resultsRequestTime = new TimeStat();

    @Inject
    public MyParallelDataResource(TaskManager taskManager,
            @ForAsyncHttp BoundedExecutor responseExecutor,
            @ForAsyncHttp ScheduledExecutorService timeoutExecutor)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @GET
    @Path("{taskId}/data/{bufferId}/{token}")
    @Produces(APPLICATION_JSON)
    public void getResults(@PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @PathParam("token") final long token,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
            throws InterruptedException
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        long start = System.nanoTime();
        CompletableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, bufferId, token, MAX_RESULT_SIZE);
        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        bufferResultFuture = addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(taskManager.getTaskInstanceId(taskId), token, false),
                waitTime,
                timeoutExecutor);
        CompletableFuture<Response> responseFuture = bufferResultFuture.thenApply(result -> {
            List<Page> pages = result.getPages();
            Iterable<List<Object>> data = null;
            if (!pages.isEmpty()) {
                List<Type> returnTypes = taskManager.getOutputTypes(taskId);
                Session session = taskManager.getSession(taskId);
                checkState(session != null);
                ConnectorSession connectorSession = session.toConnectorSession();

                long bytes = 0;
                ImmutableList.Builder<RowIterable> resultPages = ImmutableList.builder();
                for (Page page : pages) {
                    if (page != null) {
                        bytes += page.getSizeInBytes();
                        resultPages.add(new RowIterable(connectorSession, returnTypes, page));
                    }
                }
                if (bytes > 0) {
                    data = Iterables.concat(resultPages.build());
                }
            }
            if (result.isBufferComplete() && pages.isEmpty()) {
                taskManager.abortTaskResults(taskId, bufferId);
            }
            URI nextUri = result.isBufferComplete() && pages.isEmpty() ? null : uriBuilderFrom(uriInfo.getBaseUri())
                    .appendPath("v1")
                    .appendPath("paralleldata")
                    .appendPath(taskId.toString())
                    .appendPath("data")
                    .appendPath(bufferId.toString())
                    .appendPath(String.valueOf(result.getNextToken()))
                    .build();

            ParallelDataResults parallelDataResults = new ParallelDataResults(taskId.toString(), taskManager.getOutputColumns(taskId), data);
            return Response.ok(parallelDataResults).header(PRESTO_NEXT_URI, nextUri).build();
        });

        // For hard timeout, add an additional 5 seconds to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + 5000, MILLISECONDS);
        bindAsyncResponse(asyncResponse, responseFuture, responseExecutor)
                .withTimeout(timeout, () -> Response
                        .ok(new ParallelDataResults(taskId.toString(), taskManager.getOutputColumns(taskId), null))
                        .header(PRESTO_NEXT_URI, uriInfo.getRequestUri())
                        .build());

        responseFuture.whenComplete((response, exception) -> readFromOutputBufferTime.add(Duration.nanosSince(start)));
        asyncResponse.register((CompletionCallback) throwable -> resultsRequestTime.add(Duration.nanosSince(start)));
    }

    @DELETE
    @Path("{taskId}/data/{bufferId}/{token}")
    @Produces(APPLICATION_JSON)
    public Response abortResults(@PathParam("taskId") TaskId taskId, @PathParam("bufferId") OutputBufferId bufferId, @PathParam("token") long token, @Context UriInfo uriInfo)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        TaskInfo taskInfo = taskManager.abortTaskResults(taskId, bufferId);
        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }
        return Response.ok(taskInfo).build();
    }

    @Managed
    @Nested
    public TimeStat getReadFromOutputBufferTime()
    {
        return readFromOutputBufferTime;
    }

    @Managed
    @Nested
    public TimeStat getResultsRequestTime()
    {
        return resultsRequestTime;
    }

    private static boolean shouldSummarize(UriInfo uriInfo)
    {
        return uriInfo.getQueryParameters().containsKey("summarize");
    }

    private static Duration randomizeWaitTime(Duration waitTime)
    {
        // Randomize in [T/2, T], so wait is not near zero and the client-supplied max wait time is respected
        long halfWaitMillis = waitTime.toMillis() / 2;
        return new Duration(halfWaitMillis + ThreadLocalRandom.current().nextLong(halfWaitMillis), MILLISECONDS);
    }
}
