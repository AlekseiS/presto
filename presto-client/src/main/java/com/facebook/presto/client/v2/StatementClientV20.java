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
package com.facebook.presto.client.v2;

import com.facebook.presto.client.ClientException;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.CreateQueryRequest;
import com.facebook.presto.client.CreateQuerySession;
import com.facebook.presto.client.DataResults;
import com.facebook.presto.client.JsonResponse;
import com.facebook.presto.client.OkHttpUtil.NullCallback;
import com.facebook.presto.client.QueryActions;
import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
class StatementClientV20
        implements StatementClient
{
    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    private static final JsonCodec<CreateQueryRequest> CREATE_QUERY_REQUEST_JSON_CODEC = jsonCodec(CreateQueryRequest.class);
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final JsonCodec<DataResults> DATA_RESULTS_JSON_CODEC = jsonCodec(DataResults.class);
    private static final QueryData EMPTY_DATA = new DataResults(null, null);

    private static final String USER_AGENT_VALUE = StatementClientV20.class.getSimpleName() +
            "/" +
            firstNonNull(StatementClientV20.class.getPackage().getImplementationVersion(), "unknown");

    private final OkHttpClient httpClient;
    private final boolean debug;
    private final String query;
    private final AtomicReference<QueryStatusInfo> currentStatus = new AtomicReference<>();
    // initialize with null results because it's expected to never be null
    private final AtomicReference<QueryData> currentData = new AtomicReference<>(EMPTY_DATA);
    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();
    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();
    private final AtomicReference<String> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean gone = new AtomicBoolean();
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final TimeZoneKey timeZone;
    private final long requestTimeoutNanos;
    private final String user;
    private final AtomicReference<List<Column>> columns = new AtomicReference<>();
    private final List<DataClient> dataClients = synchronizedList(new LinkedList<>());
    private final AtomicBoolean clientsCreated = new AtomicBoolean();

    private final BlockingQueue<QueryData> dataQueue = new LinkedBlockingQueue<>();

    private class DataClient
    {
        private final AtomicReference<URI> lastUri = new AtomicReference<>();
        private final AtomicReference<URI> nextUri;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final Callback callback = new MyCallback();
        private final AtomicBoolean inProgress = new AtomicBoolean();

        public DataClient(URI nextUri)
        {
            this.nextUri = new AtomicReference<>(nextUri);
        }

        public boolean hasNext()
        {
            return nextUri.get() != null;
        }

        /*
                public boolean advanceData()
                {
                    if (isClosed() || nextUri.get() == null) {
                        return false;
                    }
                    // TODO: make maxSize in TaskResource nullable
                    HttpUrl url = HttpUrl.get(nextUri.get()).newBuilder()
                            .addQueryParameter("maxSize", "1MB")
                            .addQueryParameter("maxWaitTime", "10s")
                            .build();
                    Request request = prepareRequest(url, user).build();

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
                                throw new RuntimeException("StatementClient data thread was interrupted");
                            }
                        }
                        attempts++;

                        JsonResponse<DataResults> response;
                        try {
                            response = JsonResponse.execute(DATA_RESULTS_JSON_CODEC, httpClient, request);
                        }
                        catch (RuntimeException e) {
                            cause = e;
                            continue;
                        }

                        if (response.getStatusCode() == HTTP_OK && response.hasValue()) {
                            processDataResponse(response.getValue());
                            return true;
                        }

                        if (response.getStatusCode() != HTTP_UNAVAILABLE) {
                            throw requestFailedException("fetching next", request, response);
                        }
                    }
                    while (((System.nanoTime() - start) < requestTimeoutNanos) && !isClosed());

                    gone.set(true);
                    throw new RuntimeException("Error fetching next", cause);
                }
        */
        private void processDataResponse(DataResults dataResults)
        {
            List<Column> resultColumns = columns.get();
            checkState(resultColumns != null, "columns are not ready");
            lastUri.set(nextUri.get());
            nextUri.set(dataResults.getNextUri());
            // queue capacity should be set in a way that it never throws
            dataQueue.add(dataResults.withFixedData(resultColumns));
        }

        public boolean scheduleRequest()
        {
            if (nextUri.get() == null) {
                if (!isClosed()) {
                    close();
                }
                return false;
            }
            HttpUrl url = HttpUrl.get(nextUri.get()).newBuilder()
                    .addQueryParameter("maxSize", "1MB")
                    .addQueryParameter("maxWaitTime", "1s")
                    .build();
            Request request = prepareRequest(url, user).build();
            inProgress.set(true);
            httpClient.newCall(request).enqueue(callback);
            return true;
        }

        public boolean isInProgress()
        {
            return inProgress.get();
        }

        private final class MyCallback
                implements Callback
        {
            @Override
            public void onFailure(Call call, IOException e)
            {
                // TODO: add retries
                // TODO: schedule another request globally
                inProgress.set(false);
                gone.set(true);
            }

            @Override
            public void onResponse(Call call, Response response)
                    throws IOException
            {
                inProgress.set(false);
                try (Response ignored = response) {
                    if ((response.code() == 307) || (response.code() == 308)) {
                        String location = response.header(LOCATION);
                        if (location != null) {
                            Request newRequest = call.request().newBuilder().url(location).build();
                            inProgress.set(true);
                            httpClient.newCall(newRequest).enqueue(callback);
                            return;
                        }
                    }
                    DataResults results = DATA_RESULTS_JSON_CODEC.fromJson(response.body().bytes());
                    processDataResponse(results);
                }
            }
        }

        public void close()
        {
            if (!closed.getAndSet(true)) {
                URI uri = nextUri.get() != null ? nextUri.get() : lastUri.get();
                checkState(uri != null, "uri used to close data must be not null");
                // TODO: make sure call goes through
                httpDelete(uri);
            }
        }

        private boolean isClosed()
        {
            return closed.get();
        }
    }

    public StatementClientV20(OkHttpClient httpClient, ClientSession session, String query)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");

        this.httpClient = httpClient;
        this.debug = session.isDebug();
        this.timeZone = session.getTimeZone();
        this.query = query;
        this.requestTimeoutNanos = session.getClientRequestTimeout().roundTo(NANOSECONDS);
        this.user = session.getUser();

        Request request = buildQueryRequest(session, query);

        JsonResponse<QueryResults> response = JsonResponse.execute(QUERY_RESULTS_CODEC, httpClient, request);
        if (response.getStatusCode() != HTTP_OK || !response.hasValue()) {
            throw requestFailedException("starting query", request, response);
        }

        processStatusResponse(response.getValue());
    }

    private static Request buildQueryRequest(ClientSession session, String query)
    {
        HttpUrl url = HttpUrl.get(session.getServer());
        if (url == null) {
            throw new ClientException("Invalid server URL: " + session.getServer());
        }
        url = url.newBuilder().encodedPath("/v2/statement").build();

        CreateQueryRequest createQueryRequest = new CreateQueryRequest(
                new CreateQuerySession(
                        session.getCatalog(),
                        session.getSchema(),
                        session.getUser(),
                        session.getSource(),
                        USER_AGENT_VALUE,
                        session.getTimeZone() != null ? session.getTimeZone().getId() : null,
                        session.getLocale() != null ? session.getLocale().toLanguageTag() : null,
                        null,
                        session.getProperties(),
                        session.getPreparedStatements(),
                        session.getTransactionId(),
                        true,
                        session.getClientInfo()),
                query);
        return prepareRequest(url, session.getUser())
                .post(RequestBody.create(MEDIA_TYPE_JSON, CREATE_QUERY_REQUEST_JSON_CODEC.toJsonBytes(createQueryRequest)))
                .build();
    }

    @Override
    public String getQuery()
    {
        return query;
    }

    @Override
    public TimeZoneKey getTimeZone()
    {
        return timeZone;
    }

    @Override
    public boolean isDebug()
    {
        return debug;
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public boolean isGone()
    {
        return gone.get();
    }

    /**
     * Means that query result has an error.
     */
    @Override
    public boolean isFailed()
    {
        return currentStatus.get().getError() != null;
    }

    @Override
    public QueryStatusInfo currentStatusInfo()
    {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return currentStatus.get();
    }

    @Override
    public QueryData currentData()
    {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return currentData.get();
    }

    @Override
    public QueryStatusInfo finalStatusInfo()
    {
        checkState((!isValid()) || isFailed(), "current position is still valid");
        return currentStatus.get();
    }

    @Override
    public Map<String, String> getSetSessionProperties()
    {
        return unmodifiableMap(setSessionProperties);
    }

    @Override
    public Set<String> getResetSessionProperties()
    {
        return unmodifiableSet(resetSessionProperties);
    }

    @Override
    public Map<String, String> getAddedPreparedStatements()
    {
        return unmodifiableMap(addedPreparedStatements);
    }

    @Override
    public Set<String> getDeallocatedPreparedStatements()
    {
        return unmodifiableSet(deallocatedPreparedStatements);
    }

    @Override
    @Nullable
    public String getStartedTransactionId()
    {
        return startedTransactionId.get();
    }

    @Override
    public boolean isClearTransactionId()
    {
        return clearTransactionId.get();
    }

    /**
     * Means that it can get more data.
     */
    @Override
    public boolean isValid()
    {
        return valid.get() && (!isGone()) && (!isClosed());
    }

    private static Request.Builder prepareRequest(HttpUrl url, String user)
    {
        // TODO: find out if user header is required
        return new Request.Builder()
                .addHeader(PRESTO_USER, user)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                // TODO: use a constant
                .addHeader(ACCEPT, "application/json")
                .url(url);
    }

    @Override
    public boolean advance()
    {
        return advanceStatus() && advanceData();
    }

    private boolean advanceData()
    {
        currentData.set(EMPTY_DATA);
        if (!clientsCreated.get()) {
            // no clients yet
            return true;
        }
        if (dataClients.isEmpty() && dataQueue.isEmpty()) {
            // all clients finished
            return true;
        }
        scheduleDataRequestIfNecessary();
        try {
            QueryData data = dataQueue.poll(requestTimeoutNanos, TimeUnit.NANOSECONDS);
            currentData.set(data == null ? EMPTY_DATA : data);
            return true;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            gone.set(true);
            throw new RuntimeException("Interrupted waiting for data", e);
        }
    }

    private boolean scheduleDataRequestIfNecessary()
    {
        while (!dataClients.isEmpty()) {
            DataClient client = dataClients.remove(0);
            if (!client.hasNext()) {
                client.close();
            }
            else if (!client.isInProgress()) {
                client.scheduleRequest();
                dataClients.add(client);
                return true;
            }
            else {
                dataClients.add(client);
            }
        }
        return false;
    }

    private boolean advanceStatus()
    {
        URI nextUri = currentStatusInfo().getNextUri();
        if (isClosed() || nextUri == null) {
            valid.set(false);
            return false;
        }

        Request request = prepareRequest(HttpUrl.get(nextUri), user).build();

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

            JsonResponse<QueryResults> response;
            try {
                response = JsonResponse.execute(QUERY_RESULTS_CODEC, httpClient, request);
            }
            catch (RuntimeException e) {
                cause = e;
                continue;
            }

            if (response.getStatusCode() == HTTP_OK && response.hasValue()) {
                processStatusResponse(response.getValue());
                return true;
            }

            if (response.getStatusCode() != HTTP_UNAVAILABLE) {
                throw requestFailedException("fetching next", request, response);
            }
        }
        while (((System.nanoTime() - start) < requestTimeoutNanos) && !isClosed());

        gone.set(true);
        throw new RuntimeException("Error fetching next", cause);
    }

    private void processStatusResponse(QueryResults results)
    {
        checkState(results.getData() == null, "data must not be present in v2");
        if (results.getColumns() != null && columns.get() == null) {
            columns.set(results.getColumns());
        }
        if (results.getDataUris() != null && !clientsCreated.getAndSet(true)) {
            for (URI dataUri : results.getDataUris()) {
                DataClient client = new DataClient(dataUri);
                dataClients.add(client);
                client.scheduleRequest();
            }
        }

        QueryActions actions = results.getActions();
        if (actions == null) {
            currentStatus.set(results);
            return;
        }
        if (actions.getSetSessionProperties() != null) {
            setSessionProperties.putAll(actions.getSetSessionProperties());
        }
        if (actions.getClearSessionProperties() != null) {
            resetSessionProperties.addAll(actions.getClearSessionProperties());
        }
        if (actions.getAddedPreparedStatements() != null) {
            addedPreparedStatements.putAll(actions.getAddedPreparedStatements());
        }
        if (actions.getDeallocatedPreparedStatements() != null) {
            deallocatedPreparedStatements.addAll(actions.getDeallocatedPreparedStatements());
        }
        if (actions.getStartedTransactionId() != null) {
            startedTransactionId.set(actions.getStartedTransactionId());
        }
        if (actions.isClearTransactionId() != null && actions.isClearTransactionId()) {
            clearTransactionId.set(true);
        }
        currentStatus.set(results);
    }

    private RuntimeException requestFailedException(String task, Request request, JsonResponse<?> response)
    {
        gone.set(true);
        if (!response.hasValue()) {
            if (response.getStatusCode() == HTTP_UNAUTHORIZED) {
                return new ClientException("Authentication failed" +
                        Optional.ofNullable(response.getStatusMessage())
                                .map(message -> ": " + message)
                                .orElse(""));
            }
            return new RuntimeException(
                    format("Error %s at %s returned an invalid response: %s [Error: %s]", task, request.url(), response, response.getResponseBody()),
                    response.getException());
        }
        return new RuntimeException(format("Error %s at %s returned HTTP %s", task, request.url(), response.getStatusCode()));
    }

    @Override
    public void cancelLeafStage()
    {
        checkState(!isClosed(), "client is closed");

        URI uri = currentStatusInfo().getPartialCancelUri();
        if (uri != null) {
            httpDelete(uri);
        }
    }

    @Override
    public void close()
    {
        if (!closed.getAndSet(true)) {
            // close data download
            for (DataClient dataClient : dataClients) {
                dataClient.close();
            }
            dataClients.clear();
            // close query
            URI uri = currentStatus.get().getNextUri();
            if (uri != null) {
                httpDelete(uri);
            }
        }
    }

    private void httpDelete(URI uri)
    {
        Request request = prepareRequest(HttpUrl.get(uri), user)
                .delete()
                .build();
        httpClient.newCall(request).enqueue(new NullCallback());
    }
}
