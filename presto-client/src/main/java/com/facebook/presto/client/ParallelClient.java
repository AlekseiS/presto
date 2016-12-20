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
package com.facebook.presto.client;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_NEXT_URI;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.Family;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class ParallelClient
        implements Closeable
{
    private static final Logger log = Logger.get(ParallelClient.class);

    private static final Splitter SESSION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();
    private static final String USER_AGENT_VALUE = ParallelClient.class.getSimpleName() +
            "/" +
            firstNonNull(ParallelClient.class.getPackage().getImplementationVersion(), "unknown");

    private final HttpClient httpClient;
    private final FullJsonResponseHandler<ParallelStatus> statusResponseHandler;
    private final FullJsonResponseHandler<ParallelDataResults> taskResponseHandler;
    private final boolean debug;
    private final String query;
    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();
    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();
    private final AtomicReference<String> startedtransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean gone = new AtomicBoolean();
    private final String timeZoneId;
    private final long requestTimeoutNanos;
    private final String user;

    private final AtomicReference<State> state = new AtomicReference<State>();

    private interface State
    {
        boolean advance();

        StatementStats getStats();

        boolean isFailed();

        QueryResults current();

        QueryResults finalResults();

        void close();

        boolean isQueueEmpty();
    }

    private class TaskDownload
            implements Runnable, AutoCloseable
    {
        private final URI initialUri;
        private final BlockingQueue<ParallelDataResults> queue;
        private final AtomicBoolean taskClosed = new AtomicBoolean();
        private URI lastUri;

        public TaskDownload(URI initialUri, BlockingQueue<ParallelDataResults> queue)
        {
            this.initialUri = initialUri;
            this.queue = queue;
        }

        @Override
        public void run()
        {
            try {
                URI nextUri = initialUri;
                while (!closed.get() && nextUri != null) {
                    lastUri = nextUri;
                    JsonResponse<ParallelDataResults> result = advanceInternal(nextUri, taskResponseHandler);
                    String nextHeader = result.getHeader(PRESTO_NEXT_URI);
                    nextUri = nextHeader == null ? null : URI.create(nextHeader);
                    if (result.hasValue()) {
                        queue.put(result.getValue());
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Task download interrupted", e);
            }
            catch (Exception e) {
                log.error(e, "Error downloading task results");
                throw e;
            }
            finally {
                try {
                    close();
                }
                catch (Exception e) {
                    log.warn("Error closing a task", e);
                }
            }
        }

        @Override
        public void close()
                throws Exception
        {
            if (lastUri != null && !taskClosed.getAndSet(true)) {
                Request request = prepareRequest(prepareDelete(), lastUri).build();
                log.info("Sending delete to %s", lastUri);
                httpClient.executeAsync(request, createStatusResponseHandler());
            }
        }
    }

    private class StatsDownload
            implements Runnable, AutoCloseable
    {
        private final URI initialUri;
        private final AtomicReference<ParallelStatus> statusResult;
        private final AtomicBoolean finished;
        private URI lastUri;

        public StatsDownload(URI initialUri, AtomicReference<ParallelStatus> statusResult, AtomicBoolean finished)
        {
            this.initialUri = initialUri;
            this.statusResult = statusResult;
            this.finished = finished;
        }

        @Override
        public void run()
        {
            try {
                URI nextUri = initialUri;
                while (nextUri != null) {
                    lastUri = nextUri;
                    JsonResponse<ParallelStatus> result = advanceInternal(nextUri, statusResponseHandler);
                    processQueryResponse(result);
                    nextUri = result.getValue().getNextUri();
                    statusResult.set(result.getValue());
                }
            }
            finally {
                finished.set(true);
                try {
                    close();
                }
                catch (Exception e) {
                    log.warn("Error closing stats download", e);
                }
            }
        }

        @Override
        public void close()
                throws Exception
        {
            if (lastUri != null && !closed.getAndSet(true)) {
                Request request = prepareRequest(prepareDelete(), lastUri).build();
                log.info("Sending stats delete to %s", lastUri);
                httpClient.executeAsync(request, createStatusResponseHandler());
            }
        }
    }

    private class CoordinatorAndTasks
            implements State
    {
        private final AtomicReference<ParallelStatus> currentStatus = new AtomicReference<>();
        private final ExecutorService statusExecutor = Executors.newSingleThreadExecutor();
        private final ExecutorService taskExecutors = Executors.newCachedThreadPool();
        private final BlockingQueue<ParallelDataResults> queue;
        private final AtomicBoolean finished = new AtomicBoolean();

        public CoordinatorAndTasks(ParallelStatus currentQueryResults)
        {
            List<URI> dataUris = currentQueryResults.getDataUris();
            checkState(dataUris != null && !dataUris.isEmpty());
            currentStatus.set(currentQueryResults);
            queue = new ArrayBlockingQueue<>(dataUris.size() * 2);
            // add a dummy empty task result
            checkState(queue.offer(new ParallelDataResults(currentQueryResults.getId(), null, null)));
            for (URI taskUri : dataUris) {
                taskExecutors.submit(new TaskDownload(taskUri, queue));
            }
            statusExecutor.submit(new StatsDownload(currentQueryResults.getNextUri(), currentStatus, finished));
        }

        @Override
        public boolean advance()
        {
            checkState(queue.poll() != null);
            while (!finished.get()) {
                if (queue.peek() != null) {
                    return true;
                }
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return false;
        }

        //TODO: gone flag
        private ParallelStatus currentStatus()
        {
            return currentStatus.get();
        }

        private ParallelDataResults currentData()
        {
            ParallelDataResults data = queue.peek();
            checkState(data != null);
            return data;
        }

        @Override
        public StatementStats getStats()
        {
            return currentStatus.get().getStats();
        }

        @Override
        public boolean isFailed()
        {
            return currentStatus.get().getError() != null;
        }

        @Override
        public QueryResults current()
        {
            ParallelStatus status = currentStatus();
            ParallelDataResults data = currentData();
            return new QueryResults(status.getId(),
                    status.getInfoUri(),
                    status.getPartialCancelUri(),
                    status.getNextUri(),
                    data.getColumns(),
                    data.getData(),
                    status.getStats(),
                    status.getError(),
                    null,
                    null);
        }

        @Override
        public QueryResults finalResults()
        {
            ParallelStatus status = currentStatus();
            return new QueryResults(status.getId(),
                    status.getInfoUri(),
                    status.getPartialCancelUri(),
                    status.getNextUri(),
                    null,
                    null,
                    status.getStats(),
                    status.getError(),
                    null,
                    null);
        }

        @Override
        public void close()
        {
            //TODO: send close requests
            taskExecutors.shutdownNow();
            statusExecutor.shutdownNow();
        }

        @Override
        public boolean isQueueEmpty()
        {
            return queue.isEmpty();
        }
    }

    public ParallelClient(HttpClient httpClient, ClientSession session, String query)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");

        this.httpClient = httpClient;
        this.statusResponseHandler = createFullJsonResponseHandler(jsonCodec(ParallelStatus.class));
        this.taskResponseHandler = createFullJsonResponseHandler(jsonCodec(ParallelDataResults.class));
        this.debug = session.isDebug();
        this.timeZoneId = session.getTimeZoneId();
        this.query = query;
        this.requestTimeoutNanos = session.getClientRequestTimeout().roundTo(NANOSECONDS);
        this.user = session.getUser();

        Request request = buildQueryRequest(session, query);
        JsonResponse<ParallelStatus> response = httpClient.execute(request, statusResponseHandler);

        if (response.getStatusCode() != HttpStatus.OK.code() || !response.hasValue()) {
            throw requestFailedException("starting query", request, response);
        }

        processQueryResponse(response);
        state.set(new CoordinatorAndTasks(response.getValue()));
    }

    private Request buildQueryRequest(ClientSession session, String query)
    {
        Request.Builder builder = prepareRequest(preparePost(), uriBuilderFrom(session.getServer()).replacePath("/v1/parallel").build())
                .setBodyGenerator(createStaticBodyGenerator(query, UTF_8));

        if (session.getSource() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SOURCE, session.getSource());
        }
        if (session.getCatalog() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SCHEMA, session.getSchema());
        }
        builder.setHeader(PrestoHeaders.PRESTO_TIME_ZONE, session.getTimeZoneId());
        if (session.getLocale() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_LANGUAGE, session.getLocale().toLanguageTag());
        }

        Map<String, String> property = session.getProperties();
        for (Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_SESSION, entry.getKey() + "=" + entry.getValue());
        }

        Map<String, String> statements = session.getPreparedStatements();
        for (Entry<String, String> entry : statements.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_PREPARED_STATEMENT, urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()));
        }

        builder.setHeader(PrestoHeaders.PRESTO_TRANSACTION_ID, session.getTransactionId() == null ? "NONE" : session.getTransactionId());

        return builder.build();
    }

    public String getQuery()
    {
        return query;
    }

    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    public boolean isDebug()
    {
        return debug;
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    public boolean isGone()
    {
        return gone.get();
    }

    public boolean isFailed()
    {
        return state.get().isFailed();
    }

    public StatementStats getStats()
    {
        return state.get().getStats();
    }

    public QueryResults current()
    {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return state.get().current();
    }

    public QueryResults finalResults()
    {
        checkState((!isValid()) || isFailed(), "current position is still valid");
        return state.get().finalResults();
    }

    public Map<String, String> getSetSessionProperties()
    {
        return ImmutableMap.copyOf(setSessionProperties);
    }

    public Set<String> getResetSessionProperties()
    {
        return ImmutableSet.copyOf(resetSessionProperties);
    }

    public Map<String, String> getAddedPreparedStatements()
    {
        return ImmutableMap.copyOf(addedPreparedStatements);
    }

    public Set<String> getDeallocatedPreparedStatements()
    {
        return ImmutableSet.copyOf(deallocatedPreparedStatements);
    }

    public String getStartedtransactionId()
    {
        return startedtransactionId.get();
    }

    public boolean isClearTransactionId()
    {
        return clearTransactionId.get();
    }

    public boolean isValid()
    {
        return !isGone() && (!isClosed() || !state.get().isQueueEmpty());
    }

    private Request.Builder prepareRequest(Request.Builder builder, URI nextUri)
    {
        builder.setHeader(PrestoHeaders.PRESTO_USER, user);
        builder.setHeader(USER_AGENT, USER_AGENT_VALUE)
                .setUri(nextUri);

        return builder;
    }

    public boolean advance()
    {
        return state.get().advance();
    }

    private <T> JsonResponse<T> advanceInternal(URI nextUri, FullJsonResponseHandler<T> responseHandler)
    {
        Request request = prepareRequest(prepareGet(), nextUri).build();

        Exception cause = null;
        long start = System.nanoTime();
        long attempts = 0;

        do {
            // back-off on retry
            if (attempts > 0) {
                try {
                    MILLISECONDS.sleep(attempts * 100);
                }
                catch (InterruptedException e) {
                    try {
                        close();
                    }
                    finally {
                        Thread.currentThread().interrupt();
                    }
                    throw new RuntimeException("Client thread was interrupted");
                }
            }
            attempts++;

            JsonResponse<T> response;
            try {
                response = httpClient.execute(request, responseHandler);
            }
            catch (RuntimeException e) {
                cause = e;
                continue;
            }

            if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue() || response.getStatusCode() == HttpStatus.NO_CONTENT.code()) {
                return response;
            }

            if (response.getStatusCode() != HttpStatus.SERVICE_UNAVAILABLE.code()) {
                throw requestFailedException("fetching next", request, response);
            }
        }
        while (((System.nanoTime() - start) < requestTimeoutNanos) && !isClosed());

        gone.set(true);
        throw new RuntimeException("Error fetching next", cause);
    }

    private static URI createTaskCloseUri(URI uri)
    {
        String path = uri.getPath();
        int from = path.length() - 1;
        if (path.charAt(from) == '/') {
            from--;
        }
        int idx = path.lastIndexOf('/', from);
        checkState(idx >= 0);
        String newPath = path.substring(0, idx).replace("download", "results");
        return uriBuilderFrom(uri).replacePath(newPath).build();
    }

    private void processQueryResponse(JsonResponse<ParallelStatus> response)
    {
        for (String setSession : response.getHeaders(PRESTO_SET_SESSION)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setSession);
            if (keyValue.size() != 2) {
                continue;
            }
            setSessionProperties.put(keyValue.get(0), keyValue.size() > 1 ? keyValue.get(1) : "");
        }
        for (String clearSession : response.getHeaders(PRESTO_CLEAR_SESSION)) {
            resetSessionProperties.add(clearSession);
        }

        for (String entry : response.getHeaders(PRESTO_ADDED_PREPARE)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(entry);
            if (keyValue.size() != 2) {
                continue;
            }
            addedPreparedStatements.put(urlDecode(keyValue.get(0)), urlDecode(keyValue.get(1)));
        }
        for (String entry : response.getHeaders(PRESTO_DEALLOCATED_PREPARE)) {
            deallocatedPreparedStatements.add(urlDecode(entry));
        }

        String startedTransactionId = response.getHeader(PRESTO_STARTED_TRANSACTION_ID);
        if (startedTransactionId != null) {
            this.startedtransactionId.set(startedTransactionId);
        }
        if (response.getHeader(PRESTO_CLEAR_TRANSACTION_ID) != null) {
            clearTransactionId.set(true);
        }
    }

    private RuntimeException requestFailedException(String task, Request request, JsonResponse<?> response)
    {
        gone.set(true);
        if (!response.hasValue()) {
            return new RuntimeException(
                    format("Error %s at %s returned an invalid response: %s [Error: %s]", task, request.getUri(), response, response.getResponseBody()),
                    response.getException());
        }
        return new RuntimeException(format("Error %s at %s returned %s: %s", task, request.getUri(), response.getStatusCode(), response.getStatusMessage()));
    }

    public boolean cancelLeafStage(Duration timeout)
    {
        checkState(!isClosed(), "client is closed");

        URI uri = current().getPartialCancelUri();
        if (uri == null) {
            return false;
        }

        Request request = prepareRequest(prepareDelete(), uri).build();

        HttpResponseFuture<StatusResponse> response = httpClient.executeAsync(request, createStatusResponseHandler());
        try {
            StatusResponse status = response.get(timeout.toMillis(), MILLISECONDS);
            return familyForStatusCode(status.getStatusCode()) == Family.SUCCESSFUL;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
        catch (TimeoutException e) {
            return false;
        }
    }

    @Override
    public void close()
    {
        state.get().close();
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    private static String urlDecode(String value)
    {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
}
