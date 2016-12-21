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

import com.facebook.presto.Session;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.ParallelStatus;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.PARALLEL_OUTPUT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_NEXT_URI;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PARALLEL_COUNT;
import static com.facebook.presto.server.ResourceUtil.assertRequest;
import static com.facebook.presto.server.ResourceUtil.badRequest;
import static com.facebook.presto.server.ResourceUtil.createSessionForRequest;
import static com.facebook.presto.server.ResourceUtil.trimEmptyToNull;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;

@Path("/v1/parallel")
public class MyParallelResource
{
    private static final Logger log = Logger.get(MyParallelResource.class);

    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();

    private final QueryManager queryManager;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final QueryIdGenerator queryIdGenerator;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("query-purger"));

    @Inject
    public MyParallelResource(
            QueryManager queryManager,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            QueryIdGenerator queryIdGenerator)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");

        queryPurger.scheduleWithFixedDelay(new PurgeQueriesRunnable(queries, queryManager), 200, 200, MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryPurger.shutdownNow();
    }

    @POST
    @Produces(APPLICATION_JSON)
    public Response createQuery(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        assertRequest(!isNullOrEmpty(statement), "SQL statement is empty");
        int parallelCount = 3; //getParallelCount(servletRequest);

        Session session = Session.builder(createSessionForRequest(servletRequest, transactionManager, accessControl, sessionPropertyManager, queryIdGenerator.createNextQueryId()))
                .setSystemProperty(PARALLEL_OUTPUT, String.valueOf(true))
                .setSystemProperty(HASH_PARTITION_COUNT, String.valueOf(parallelCount))
                .build();

        Query query = new Query(session, statement, queryManager);
        queries.put(query.getQueryId(), query);

        // create initial set of task download URIs pointing at the coordinator
        // until task outputs are ready
        ImmutableList.Builder<URI> dataUris = ImmutableList.builder();
        for (int i = 0; i < parallelCount; i++) {
            dataUris.add(uriInfo.getRequestUriBuilder()
                    .path(query.getQueryId().toString())
                    .path(String.valueOf(i))
                    .replaceQuery("")
                    .build());
        }

        return getQueryStatus(query, uriInfo, dataUris.build(), new Duration(1, MILLISECONDS));
    }

    @GET
    @Path("{queryId}")
    @Produces(APPLICATION_JSON)
    public Response getStatus(
            @PathParam("queryId") QueryId queryId,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        Query query = queries.get(queryId);
        if (query == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        return getQueryStatus(query, uriInfo, null, wait);
    }

    private static Response getQueryStatus(Query query, UriInfo uriInfo, List<URI> dataUris, Duration wait)
            throws InterruptedException
    {
        ParallelStatus queryStatus = query.getStatus(uriInfo, dataUris, wait);
        ResponseBuilder response = Response.ok(queryStatus);
        return response.build();
    }

    @GET
    @Path("{queryId}/{output}")
    @Produces(APPLICATION_JSON)
    public Response getOutputStart(
            @PathParam("queryId") QueryId queryId,
            @PathParam("output") int output,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        //TODO: add throttling with maxWait
        Thread.sleep(100);

        Query query = queries.get(queryId);
        if (query == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        QueryInfo info = queryManager.getQueryInfo(queryId);

        URI nextUri = uriInfo.getRequestUriBuilder().replaceQuery("").build();
        Response response = Response.noContent()
                .header(PRESTO_NEXT_URI, nextUri)
                .build();

        if (!info.getOutputStage().isPresent()) {
            return response;
        }

        List<TaskInfo> tasks = info.getOutputStage().get().getTasks();
        if (tasks.size() <= output) {
            if (info.getOutputStage().get().getState() != StageState.PLANNED && info.getOutputStage().get().getState() != StageState.SCHEDULING) {
                // there are less tasks than expected, finishing this one
                return Response.noContent().build();
            }
            else {
                return response;
            }
        }
        TaskInfo taskInfo = tasks.get(output);

        OutputBufferInfo outputBuffers = taskInfo.getOutputBuffers();
        List<BufferInfo> buffers = outputBuffers.getBuffers();
        if (buffers.isEmpty() || outputBuffers.getState().canAddBuffers()) {
            // output buffer has not been created yet
            return response;
        }

        checkState(buffers.size() == 1, "Expected a single output buffer for task %s, but found %s", taskInfo.getTaskStatus().getTaskId(), buffers);

        nextUri = uriBuilderFrom(taskInfo.getTaskStatus().getSelf())
                .replacePath("/v1/paralleldata")
                .appendPath(taskInfo.getTaskStatus().getTaskId().toString())
                .appendPath("data")
                .appendPath(getOnlyElement(buffers).getBufferId().toString())
                .appendPath("0")
                .build();

        return Response.status(NO_CONTENT)
                .header(PRESTO_NEXT_URI, nextUri)
                .build();
    }

    @DELETE
    @Path("{queryId}")
    @Produces(APPLICATION_JSON)
    public Response cancelQuery(@PathParam("queryId") QueryId queryId)
    {
        Query query = queries.get(queryId);
        if (query == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        query.cancel();
        return Response.noContent().build();
    }

    @ThreadSafe
    private static class Query
    {
        private final QueryManager queryManager;
        private final QueryId queryId;

        public Query(Session session,
                String query,
                QueryManager queryManager)
        {
            requireNonNull(query, "query is null");
            requireNonNull(queryManager, "queryManager is null");

            this.queryManager = queryManager;

            QueryInfo queryInfo = queryManager.createQuery(session, query);
            queryId = queryInfo.getQueryId();
        }

        public void cancel()
        {
            queryManager.cancelQuery(queryId);
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public synchronized ParallelStatus getStatus(UriInfo uriInfo, List<URI> dataUris, Duration maxWaitTime)
                throws InterruptedException
        {
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            queryManager.recordHeartbeat(queryId);

            if (!queryInfo.getState().isDone()) {
                queryManager.waitForStateChange(queryId, queryInfo.getState(), maxWaitTime);
                queryInfo = queryManager.getQueryInfo(queryId);
            }

            // only return a next if the query is not done
            URI nextResultsUri = null;
            if (!queryInfo.isFinalQueryInfo()) {
                nextResultsUri = createNextResultsUri(uriInfo);
            }

            return new ParallelStatus(
                    queryId.toString(),
                    uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                    findCancelableLeafStage(queryInfo),
                    nextResultsUri,
                    dataUris,
                    toStatementStats(queryInfo),
                    toQueryError(queryInfo));
        }

        private synchronized URI createNextResultsUri(UriInfo uriInfo)
        {
            return uriInfo.getBaseUriBuilder().replacePath("/v1/parallel").path(queryId.toString()).replaceQuery("").build();
        }

        static StatementStats toStatementStats(QueryInfo queryInfo)
        {
            QueryStats queryStats = queryInfo.getQueryStats();
            StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

            return StatementStats.builder()
                    .setState(queryInfo.getState().toString())
                    .setQueued(queryInfo.getState() == QueryState.QUEUED)
                    .setScheduled(queryInfo.isScheduled())
                    .setNodes(globalUniqueNodes(outputStage).size())
                    .setTotalSplits(queryStats.getTotalDrivers())
                    .setQueuedSplits(queryStats.getQueuedDrivers())
                    .setRunningSplits(queryStats.getRunningDrivers())
                    .setCompletedSplits(queryStats.getCompletedDrivers())
                    .setUserTimeMillis(queryStats.getTotalUserTime().toMillis())
                    .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                    .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                    .setProcessedRows(queryStats.getRawInputPositions())
                    .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                    .setRootStage(toStageStats(outputStage))
                    .build();
        }

        private static StageStats toStageStats(StageInfo stageInfo)
        {
            if (stageInfo == null) {
                return null;
            }

            com.facebook.presto.execution.StageStats stageStats = stageInfo.getStageStats();

            ImmutableList.Builder<StageStats> subStages = ImmutableList.builder();
            for (StageInfo subStage : stageInfo.getSubStages()) {
                subStages.add(toStageStats(subStage));
            }

            Set<String> uniqueNodes = new HashSet<>();
            for (TaskInfo task : stageInfo.getTasks()) {
                URI uri = task.getTaskStatus().getSelf();
                uniqueNodes.add(uri.getHost() + ":" + uri.getPort());
            }

            return StageStats.builder()
                    .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                    .setState(stageInfo.getState().toString())
                    .setDone(stageInfo.getState().isDone())
                    .setNodes(uniqueNodes.size())
                    .setTotalSplits(stageStats.getTotalDrivers())
                    .setQueuedSplits(stageStats.getQueuedDrivers())
                    .setRunningSplits(stageStats.getRunningDrivers())
                    .setCompletedSplits(stageStats.getCompletedDrivers())
                    .setUserTimeMillis(stageStats.getTotalUserTime().toMillis())
                    .setCpuTimeMillis(stageStats.getTotalCpuTime().toMillis())
                    .setWallTimeMillis(stageStats.getTotalScheduledTime().toMillis())
                    .setProcessedRows(stageStats.getRawInputPositions())
                    .setProcessedBytes(stageStats.getRawInputDataSize().toBytes())
                    .setSubStages(subStages.build())
                    .build();
        }

        private static Set<String> globalUniqueNodes(StageInfo stageInfo)
        {
            if (stageInfo == null) {
                return ImmutableSet.of();
            }
            ImmutableSet.Builder<String> nodes = ImmutableSet.builder();
            for (TaskInfo task : stageInfo.getTasks()) {
                URI uri = task.getTaskStatus().getSelf();
                nodes.add(uri.getHost() + ":" + uri.getPort());
            }

            for (StageInfo subStage : stageInfo.getSubStages()) {
                nodes.addAll(globalUniqueNodes(subStage));
            }
            return nodes.build();
        }

        static URI findCancelableLeafStage(QueryInfo queryInfo)
        {
            // if query is running, find the leaf-most running stage
            return queryInfo.getOutputStage().map(Query::findCancelableLeafStage).orElse(null);
        }

        private static URI findCancelableLeafStage(StageInfo stage)
        {
            // if this stage is already done, we can't cancel it
            if (stage.getState().isDone()) {
                return null;
            }

            // attempt to find a cancelable sub stage
            // check in reverse order since build side of a join will be later in the list
            for (StageInfo subStage : Lists.reverse(stage.getSubStages())) {
                URI leafStage = findCancelableLeafStage(subStage);
                if (leafStage != null) {
                    return leafStage;
                }
            }

            // no matching sub stage, so return this stage
            return stage.getSelf();
        }

        static QueryError toQueryError(QueryInfo queryInfo)
        {
            FailureInfo failure = queryInfo.getFailureInfo();
            if (failure == null) {
                QueryState state = queryInfo.getState();
                if ((!state.isDone()) || (state == QueryState.FINISHED)) {
                    return null;
                }
                log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
                failure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state))).toFailureInfo();
            }

            ErrorCode errorCode;
            if (queryInfo.getErrorCode() != null) {
                errorCode = queryInfo.getErrorCode();
            }
            else {
                errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
                log.warn("Failed query %s has no error code", queryInfo.getQueryId());
            }
            return new QueryError(
                    failure.getMessage(),
                    null,
                    errorCode.getCode(),
                    errorCode.getName(),
                    errorCode.getType().toString(),
                    failure.getErrorLocation(),
                    failure);
        }
    }

    private static int getParallelCount(HttpServletRequest request)
    {
        String countHeader = trimEmptyToNull(request.getHeader(PRESTO_PARALLEL_COUNT));
        assertRequest(countHeader != null, "%s header must be set", PRESTO_PARALLEL_COUNT);
        try {
            int count = Integer.parseInt(countHeader);
            assertRequest(count > 0, "Parallel count must be > 0");
            return count;
        }
        catch (NumberFormatException e) {
            throw badRequest(PRESTO_PARALLEL_COUNT + " header is invalid");
        }
    }

    private static class PurgeQueriesRunnable
            implements Runnable
    {
        private final ConcurrentMap<QueryId, Query> queries;
        private final QueryManager queryManager;

        private PurgeQueriesRunnable(ConcurrentMap<QueryId, Query> queries, QueryManager queryManager)
        {
            this.queries = queries;
            this.queryManager = queryManager;
        }

        @Override
        public void run()
        {
            try {
                // Queries are added to the query manager before being recorded in queryIds set.
                // Therefore, we take a snapshot if queryIds before getting the live queries
                // from the query manager.  Then we remove only the queries in the snapshot and
                // not live queries set.  If we did this in the other order, a query could be
                // registered between fetching the live queries and inspecting the queryIds set.
                for (QueryId queryId : ImmutableSet.copyOf(queries.keySet())) {
                    Optional<QueryState> state = queryManager.getQueryState(queryId);

                    // forget about this query if the query manager is no longer tracking it
                    if (!state.isPresent()) {
                        queries.remove(queryId);
                    }
                }
            }
            catch (Throwable e) {
                log.warn(e, "Error removing old queries");
            }
        }
    }
}
