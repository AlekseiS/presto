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
import io.airlift.json.JsonCodec;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
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
public class StatementClient
        implements Closeable
{
    private static final Splitter SESSION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();
    private static final String USER_AGENT_VALUE = StatementClient.class.getSimpleName() +
            "/" +
            firstNonNull(StatementClient.class.getPackage().getImplementationVersion(), "unknown");

    private final HttpClient httpClient;
    private final FullJsonResponseHandler<QueryResults> responseHandler;
    private final FullJsonResponseHandler<TaskResults> taskResponseHandler;
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
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final String timeZoneId;
    private final long requestTimeoutNanos;
    private final String user;
    private final AtomicBoolean closedTask = new AtomicBoolean();
    private final AtomicReference<URI> taskCloseUri = new AtomicReference<>();

    private final AtomicReference<State> state = new AtomicReference<State>();

    private interface State
    {
        boolean advance();

        QueryResults current();

        StatementStats getStats();

        boolean isFailed();
    }

    private class CoordinatorOnly
            implements State
    {
        private final AtomicReference<QueryResults> currentQueryResults = new AtomicReference<>();

        public CoordinatorOnly(QueryResults initial)
        {
            currentQueryResults.set(initial);
        }

        @Override
        public boolean advance()
        {
            QueryResults cur = currentQueryResults.get();

            if (cur.getTaskDownloadUris() != null && !cur.getTaskDownloadUris().isEmpty()) {
                state.set(new CoordinatorAndTask(cur, getOnlyElement(cur.getTaskDownloadUris())));
                return state.get().advance();
            }

            URI nextUri = cur.getNextUri();
            if (isClosed() || (nextUri == null)) {
                valid.set(false);
                return false;
            }

            JsonResponse<QueryResults> response = advanceInternal(nextUri, responseHandler);
            checkState(response != null);
            processQueryResponse(response);
            QueryResults results = response.getValue();
            currentQueryResults.set(results);
            return true;
        }

        @Override
        public QueryResults current()
        {
            return currentQueryResults.get();
        }

        @Override
        public StatementStats getStats()
        {
            return currentQueryResults.get().getStats();
        }

        @Override
        public boolean isFailed()
        {
            return currentQueryResults.get().getError() != null;
        }
    }

    private class CoordinatorAndTask
            implements State
    {
        private final AtomicReference<QueryResults> currentQueryResults = new AtomicReference<>();
        private final AtomicReference<TaskResults> currentTaskResults = new AtomicReference<>();
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final AtomicReference<Future<?>> queryResultsFuture = new AtomicReference<>();
        private final URI initialTaskDownloadUri;

        public CoordinatorAndTask(QueryResults currentQueryResults, URI initialTaskDownloadUri)
        {
            this.currentQueryResults.set(currentQueryResults);
            this.initialTaskDownloadUri = requireNonNull(initialTaskDownloadUri);
            taskCloseUri.set(createTaskCloseUri(initialTaskDownloadUri));
        }

        @Override
        public boolean advance()
        {
            TaskResults curTask = currentTaskResults.get();
            URI nextUri = curTask != null ? curTask.getNextUri() : initialTaskDownloadUri;
            if (nextUri == null) {
                closeTask();
                // wait for a previous call for query results to complete
                getFutureValue(queryResultsFuture.get());
                executor.shutdownNow();
                state.set(new TaskDownloadedCoordinatorLeft(current()));
                return state.get().advance();
            }

            if (isClosed()) {
                valid.set(false);
                return false;
            }

            //TODO: verify thread safety
            Future<?> future = queryResultsFuture.get();
            if (future == null || future.isDone()) {
                if (future != null) {
                    // propagate exception if any
                    getFutureValue(future);
                }
                queryResultsFuture.set(executor.submit(() -> {
                    //TODO: check what happens if exception is thrown inside the future
                    URI nextQueryUri = currentQueryResults.get().getNextUri();
                    if (isClosed() || (nextQueryUri == null)) {
                        valid.set(false);
                        return false;
                    }

                    JsonResponse<QueryResults> response = advanceInternal(nextQueryUri, responseHandler);
                    checkState(response != null);
                    processQueryResponse(response);
                    QueryResults results = response.getValue();
                    currentQueryResults.set(results);
                    return true;
                }));
            }

            JsonResponse<TaskResults> response = advanceInternal(nextUri, taskResponseHandler);
            checkState(response != null);
            TaskResults results = response.getValue();
            currentTaskResults.set(results);
            return true;
        }

        @Override
        public QueryResults current()
        {
            QueryResults current = currentQueryResults.get();
            TaskResults currentTask = currentTaskResults.get();
            if (currentTask == null) {
                return current;
            }
            return new QueryResults(
                    current.getId(),
                    current.getInfoUri(),
                    current.getPartialCancelUri(),
                    current.getNextUri(),
                    currentTask.getColumns(),
                    currentTask.getData(),
                    current.getStats(),
                    current.getError(),
                    current.getUpdateType(),
                    current.getUpdateCount(),
                    current.getTaskDownloadUris()
            );
        }

        @Override
        public StatementStats getStats()
        {
            return currentQueryResults.get().getStats();
        }

        @Override
        public boolean isFailed()
        {
            return currentQueryResults.get().getError() != null;
        }
    }

    private class TaskDownloadedCoordinatorLeft
            implements State
    {
        private final AtomicReference<QueryResults> currentQueryResults = new AtomicReference<>();

        public TaskDownloadedCoordinatorLeft(QueryResults currentQueryResults)
        {
            this.currentQueryResults.set(currentQueryResults);
        }

        @Override
        public boolean advance()
        {
            URI nextUri = currentQueryResults.get().getNextUri();
            if (isClosed() || (nextUri == null)) {
                valid.set(false);
                return false;
            }

            JsonResponse<QueryResults> response = advanceInternal(nextUri, responseHandler);
            checkState(response != null);
            processQueryResponse(response);
            QueryResults results = response.getValue();
            currentQueryResults.set(results);
            return true;
        }

        @Override
        public QueryResults current()
        {
            return currentQueryResults.get();
        }

        @Override
        public StatementStats getStats()
        {
            return currentQueryResults.get().getStats();
        }

        @Override
        public boolean isFailed()
        {
            return currentQueryResults.get().getError() != null;
        }
    }

    public StatementClient(HttpClient httpClient, JsonCodec<QueryResults> queryResultsCodec, ClientSession session, String query)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(queryResultsCodec, "queryResultsCodec is null");
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");

        this.httpClient = httpClient;
        this.responseHandler = createFullJsonResponseHandler(queryResultsCodec);
        this.taskResponseHandler = createFullJsonResponseHandler(jsonCodec(TaskResults.class));
        this.debug = session.isDebug();
        this.timeZoneId = session.getTimeZoneId();
        this.query = query;
        this.requestTimeoutNanos = session.getClientRequestTimeout().roundTo(NANOSECONDS);
        this.user = session.getUser();

        Request request = buildQueryRequest(session, query);
        JsonResponse<QueryResults> response = httpClient.execute(request, responseHandler);

        if (response.getStatusCode() != HttpStatus.OK.code() || !response.hasValue()) {
            throw requestFailedException("starting query", request, response);
        }

        processQueryResponse(response);
        state.set(new CoordinatorOnly(response.getValue()));
    }

    private Request buildQueryRequest(ClientSession session, String query)
    {
        Request.Builder builder = prepareRequest(preparePost(), uriBuilderFrom(session.getServer()).replacePath("/v1/statement").build())
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
        return state.get().current();
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
        return valid.get() && (!isGone()) && (!isClosed());
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
                    throw new RuntimeException("StatementClient thread was interrupted");
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

            if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
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

    private void processQueryResponse(JsonResponse<QueryResults> response)
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
        if (!closed.getAndSet(true)) {
            URI uri = state.get().current().getNextUri();
            if (uri != null) {
                Request request = prepareRequest(prepareDelete(), uri).build();
                httpClient.executeAsync(request, createStatusResponseHandler());
            }
        }
        closeTask();
    }

    private void closeTask()
    {
        URI uri = taskCloseUri.get();
        if (uri != null && !closedTask.getAndSet(true)) {
            Request request = prepareRequest(prepareDelete(), uri).build();
            httpClient.executeAsync(request, createStatusResponseHandler());
        }
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
