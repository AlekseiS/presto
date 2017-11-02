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

import com.facebook.presto.client.AsyncOkHttpClient;
import com.facebook.presto.client.ClientException;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.CreateQueryRequest;
import com.facebook.presto.client.CreateQuerySession;
import com.facebook.presto.client.DataResults;
import com.facebook.presto.client.JsonResponse;
import com.facebook.presto.client.QueryActions;
import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.spi.type.TimeZoneKey;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.client.v2.StatusInfoResponseHandler.QUERY_RESULTS_JSON_CODEC;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

class StatementClientV21
        implements StatementClient
{
    private static final JsonCodec<CreateQueryRequest> CREATE_QUERY_REQUEST_JSON_CODEC = jsonCodec(CreateQueryRequest.class);
    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(8);
    private static final QueryData EMPTY_DATA = new DataResults(null, null);
    private static final String USER_AGENT_VALUE = StatementClientV21.class.getSimpleName() +
            "/" +
            firstNonNull(StatementClientV21.class.getPackage().getImplementationVersion(), "unknown");

    private final AsyncOkHttpClient httpClient;
    private final String query;
    private final boolean debug;
    private final TimeZoneKey timeZone;
    private final String user;
    private final HttpDataClient<QueryResults> statusClient;
    private final ExchangeClient<DataResults> exchangeClient;

    @GuardedBy("this")
    private final Map<String, String> setSessionProperties = new HashMap<>();
    @GuardedBy("this")
    private final Set<String> resetSessionProperties = new HashSet<>();
    @GuardedBy("this")
    private final Map<String, String> addedPreparedStatements = new HashMap<>();
    @GuardedBy("this")
    private final Set<String> deallocatedPreparedStatements = new HashSet<>();
    @GuardedBy("this")
    private String startedTransactionId;
    @GuardedBy("this")
    private boolean clearTransactionId;

    @GuardedBy("this")
    private List<Column> columns;
    @GuardedBy("this")
    private boolean clientsCreated;
    @GuardedBy("this")
    private QueryStatusInfo currentStatus;
    @GuardedBy("this")
    private QueryData currentData = EMPTY_DATA;
    @GuardedBy("this")
    private boolean closed;

    // can be updated by other threads
    private final AtomicReference<QueryResults> nextStatus = new AtomicReference<>();
    private final AtomicBoolean gone = new AtomicBoolean();

    public StatementClientV21(OkHttpClient okHttpClient, ClientSession session, String query)
    {
        this.httpClient = new AsyncOkHttpClient(requireNonNull(okHttpClient, "httpClient is null"));
        requireNonNull(session, "session is null");
        this.query = requireNonNull(query, "query is null");
        this.debug = session.isDebug();
        this.timeZone = session.getTimeZone();
        this.user = session.getUser();
        this.exchangeClient = new ExchangeClient<>(
                new DataSize(32, DataSize.Unit.MEGABYTE),
                new DataSize(1, DataSize.Unit.MEGABYTE),
                2,
                new Duration(5, TimeUnit.SECONDS),
                new Duration(30, TimeUnit.SECONDS),
                this.httpClient,
                EXECUTOR,
                DataResultsResponseHandler::handle);

        Request request = buildQueryRequest(session, query);

        JsonResponse<QueryResults> response = JsonResponse.execute(QUERY_RESULTS_JSON_CODEC, okHttpClient, request);
        if (response.getStatusCode() != HTTP_OK || !response.hasValue()) {
            throw requestFailedException("starting query", request, response);
        }
        this.statusClient = new HttpDataClient<>(
                this.httpClient,
                new DataSize(1, DataSize.Unit.MEGABYTE),
                new Duration(5, TimeUnit.SECONDS),
                new Duration(30, TimeUnit.SECONDS),
                response.getValue().getNextUri(),
                new StatusCallback(),
                EXECUTOR,
                StatusInfoResponseHandler::handle);
        processStatusResponse(response.getValue());
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
    public synchronized boolean isClosed()
    {
        return closed;
    }

    @Override
    public boolean isGone()
    {
        return gone.get();
    }

    @Override
    public synchronized boolean isFailed()
    {
        return currentStatus.getError() != null;
    }

    @Override
    public synchronized QueryStatusInfo currentStatusInfo()
    {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return currentStatus;
    }

    @Override
    public synchronized QueryData currentData()
    {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return currentData;
    }

    @Override
    public synchronized QueryStatusInfo finalStatusInfo()
    {
        checkState((!isValid()) || isFailed(), "current position is still valid");
        return currentStatus;
    }

    @Override
    public synchronized Map<String, String> getSetSessionProperties()
    {
        return unmodifiableMap(setSessionProperties);
    }

    @Override
    public synchronized Set<String> getResetSessionProperties()
    {
        return unmodifiableSet(resetSessionProperties);
    }

    @Override
    public synchronized Map<String, String> getAddedPreparedStatements()
    {
        return unmodifiableMap(addedPreparedStatements);
    }

    @Override
    public synchronized Set<String> getDeallocatedPreparedStatements()
    {
        return unmodifiableSet(deallocatedPreparedStatements);
    }

    @Nullable
    @Override
    public synchronized String getStartedTransactionId()
    {
        return startedTransactionId;
    }

    @Override
    public synchronized boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    @Override
    public synchronized boolean isValid()
    {
        return !closed && !gone.get() && (currentStatus.getNextUri() != null || !exchangeClient.isFinished());
    }

    @Override
    public boolean advance()
    {
        synchronized (this) {
            if (!statusClient.isRunning()) {
                statusClient.scheduleRequest();
            }
        }
        DataResults data = exchangeClient.pollPage(1, TimeUnit.SECONDS);
        synchronized (this) {
            QueryResults newStatus = nextStatus.getAndSet(null);
            if (newStatus != null) {
                processStatusResponse(newStatus);
            }
            currentData = data == null ? EMPTY_DATA : data.withFixedData(columns);
            return isValid();
        }
    }

    @Override
    public void cancelLeafStage()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void close()
    {
        if (!closed) {
            exchangeClient.close();
            statusClient.close();
            closed = true;
        }
    }

    private synchronized void processStatusResponse(@Nullable QueryResults results)
    {
        if (results == null) {
            return;
        }
        checkState(results.getData() == null, "must not have data for v2");
        currentStatus = results;

        if (results.getColumns() != null && columns == null) {
            columns = results.getColumns();
        }
        if (results.getDataUris() != null && !clientsCreated) {
            for (URI dataUri : results.getDataUris()) {
                exchangeClient.addLocation(dataUri);
            }
            exchangeClient.noMoreLocations();
            clientsCreated = true;
        }

        QueryActions actions = results.getActions();

        if (actions == null) {
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
            startedTransactionId = actions.getStartedTransactionId();
        }
        if (actions.isClearTransactionId() != null && actions.isClearTransactionId()) {
            clearTransactionId = true;
        }
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

    private static Request.Builder prepareRequest(HttpUrl url, String user)
    {
        // TODO: find out if user header is required
        return new Request.Builder()
                .addHeader(PRESTO_USER, user)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .addHeader(ACCEPT, MEDIA_TYPE_JSON.toString())
                .url(url);
    }

    private class StatusCallback
            implements HttpDataClient.ClientCallback<QueryResults>
    {
        @Override
        public boolean addPages(HttpDataClient<QueryResults> client, DataResponse<QueryResults> data)
        {
            nextStatus.set(data.getValue());
            return true;
        }

        @Override
        public void requestComplete(HttpDataClient<QueryResults> client) {}

        @Override
        public void clientFinished(HttpDataClient<QueryResults> client) {}

        @Override
        public void clientFailed(HttpDataClient<QueryResults> client, Throwable cause)
        {
            gone.set(true);
        }
    }
}
