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

import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class StatementClientV2
        implements StatementClient
{
    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json");
    private static final JsonCodec<CreateQueryRequest> CREATE_QUERY_REQUEST_JSON_CODEC = jsonCodec(CreateQueryRequest.class);
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final JsonCodec<DataResults> DATA_RESULTS_JSON_CODEC = jsonCodec(DataResults.class);
    private static final DataResults EMPTY_DATA = new DataResults(null, null);
    private static final String NO_DATA_MAX_WAIT = "1s";

    private static final String USER_AGENT_VALUE = StatementClientV2.class.getSimpleName() + "/" +
            firstNonNull(StatementClientV2.class.getPackage().getImplementationVersion(), "unknown");
    private final AsyncOkHttpClient httpClient;
    private final boolean debug;
    private final TimeZoneKey timeZone;
    private final String query;
    private final long requestTimeoutNanos;
    private final String user;

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
    private QueryStatusInfo currentStatus;
    @GuardedBy("this")
    private QueryData currentData = EMPTY_DATA;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private boolean dataClosed;
    @GuardedBy("this")
    private boolean gone;

    @GuardedBy("this")
    private List<Column> columns;
    @GuardedBy("this")
    private boolean clientsCreated;
    @GuardedBy("this")
    private URI initialStatusUri;
    @GuardedBy("this")
    private URI initialDataUri;
    @GuardedBy("this")
    private URI nextDataUri;

    @GuardedBy("this")
    private ListenableFuture<QueryResults> statusFuture;

    public StatementClientV2(OkHttpClient httpClient, ClientSession session, String query)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");

        this.httpClient = new AsyncOkHttpClient(httpClient);
        this.debug = session.isDebug();
        this.timeZone = session.getTimeZone();
        this.query = query;
        this.requestTimeoutNanos = session.getClientRequestTimeout().roundTo(NANOSECONDS);
        this.user = session.getUser();

        Request request = buildQueryRequest(session, query);

        ListenableFuture<Response> responseFuture = this.httpClient.executeAsync(request);
        ListenableFuture<Response> redirectedFuture = Futures.transformAsync(responseFuture, this::handleResponseRedirect);
        ListenableFuture<QueryResults> queryResultsFuture = Futures.transform(redirectedFuture, StatementClientV2::parseQueryResultsResponse);

        try {
            QueryResults queryResults = queryResultsFuture.get();
            initialStatusUri = queryResults.getNextUri();
            processStatusResponse(queryResults);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            close();
            throw new RuntimeException("Interrupted", e);
        }
        catch (ExecutionException e) {
            checkState(e.getCause() != null, "cause of execution exception is null");
            close();
            throw new RuntimeException("Failed to create query", e);
        }
        catch (Exception e) {
            // close query on exception
            close();
            throw e;
        }
    }

    // block of final fields

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

    // block of updatable fields

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

    @Override
    @Nullable
    public synchronized String getStartedTransactionId()
    {
        return startedTransactionId;
    }

    @Override
    public synchronized boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    // block of state-related methods

    @Override
    public synchronized boolean isClosed()
    {
        return closed;
    }

    @Override
    public synchronized boolean isGone()
    {
        return gone;
    }

    @Override
    public synchronized boolean isFailed()
    {
        return currentStatus.getError() != null;
    }

    // get current state and data

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
        checkState(!isValid() || isFailed(), "current position is still valid");
        return currentStatus;
    }

    // navigation-related methods

    @Override
    public synchronized boolean isValid()
    {
        return !gone && !closed;
    }

    @Override
    public synchronized boolean advance()
    {
        if (!isValid()) {
            return false;
        }

        URI nextStatusUri = currentStatus.getNextUri();
        if (nextStatusUri == null) {
            close();
            return false;
        }

        if (statusFuture == null) {
            HttpUrl statusUrl = HttpUrl.get(nextStatusUri);
            if (nextDataUri == null) {
                // no data yet, so wait less
                statusUrl = statusUrl.newBuilder().addQueryParameter("maxWait", NO_DATA_MAX_WAIT).build();
            }
            Request statusRequest = prepareRequest(statusUrl, user).build();
            statusFuture = Futures.transform(httpClient.executeAsync(statusRequest), StatementClientV2::parseQueryResultsResponse);
        }

        ListenableFuture<DataResults> dataFuture = null;
        if (nextDataUri != null) {
            Request dataRequest = prepareRequest(HttpUrl.get(nextDataUri), user).build();
            dataFuture = Futures.transform(httpClient.executeAsync(dataRequest), StatementClientV2::parseDataResultsResponse);
        }

        try {
            if (dataFuture == null) {
                // status only
                statusFuture.get(requestTimeoutNanos, NANOSECONDS);
            }
            else {
                // status and data
                DataResults dataResults = dataFuture.get(requestTimeoutNanos, NANOSECONDS);
                processDataResponse(dataResults);
            }
            if (statusFuture.isDone()) {
                processStatusResponse(statusFuture.get());
                statusFuture = null;
            }
            return true;
        }
        catch (InterruptedException e) {
            gone = true;
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
        catch (ExecutionException e) {
            gone = true;
            checkState(e.getCause() != null, "cause of execution exception is null");
            throw new RuntimeException("Failed to get status or data", e);
        }
        catch (TimeoutException e) {
            gone = true;
            throw new RuntimeException("Timed out waiting for status or data", e);
        }
        catch (Exception e) {
            gone = true;
            throw e;
        }
        finally {
            if (gone) {
                close();
            }
        }
    }

    // actions

    @Override
    public synchronized void cancelLeafStage()
    {
        checkState(!closed, "client is closed");

        URI uri = currentStatus.getPartialCancelUri();
        if (uri != null) {
            httpDelete(uri);
        }
    }

    @Override
    public synchronized void close()
    {
        if (!closed) {
            closeData();
            if (initialStatusUri != null) {
                httpDelete(initialStatusUri);
            }
            closed = true;
        }
    }

    // helper methods

    private synchronized void closeData()
    {
        if (!dataClosed) {
            // TODO: send delete
            if (initialDataUri != null) {
                httpDelete(initialDataUri);
            }
            dataClosed = true;
        }
    }

    private ListenableFuture<Response> handleResponseRedirect(Response response)
    {
        if (response.isRedirect()) {
            String location = response.header(LOCATION);
            if (location != null) {
                try {
                    Request newRequest = response.request().newBuilder().url(location).build();
                    ListenableFuture<Response> newResponseFuture = httpClient.executeAsync(newRequest);
                    return Futures.transformAsync(newResponseFuture, this::handleResponseRedirect);
                }
                finally {
                    response.close();
                }
            }
        }
        return Futures.immediateFuture(response);
    }

    private synchronized void processStatusResponse(QueryResults results)
    {
        checkState(results.getData() == null, "data must not be present in v2");
        if (results.getColumns() != null && columns == null) {
            columns = ImmutableList.copyOf(results.getColumns());
        }
        if (results.getDataUris() != null && !clientsCreated) {
            if (results.getDataUris().size() > 1) {
                throw new RuntimeException("Current client support only 1 data uri");
            }
            if (results.getDataUris().size() == 1) {
                initialDataUri = nextDataUri = results.getDataUris().get(0);
            }
            clientsCreated = true;
        }

        currentStatus = results;

        QueryActions actions = results.getActions();
        if (actions != null) {
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
    }

    private synchronized void processDataResponse(DataResults dataResults)
    {
        if (dataResults == null) {
            // can happen when no data uris exist
            currentData = EMPTY_DATA;
        }
        else {
            checkState(columns != null, "columns must be present");
            currentData = dataResults.withFixedData(columns);
            nextDataUri = dataResults.getNextUri();
            if (nextDataUri == null) {
                closeData();
            }
        }
    }

    private void httpDelete(URI uri)
    {
        Request request = prepareRequest(HttpUrl.get(uri), user)
                .delete()
                .build();
        // TODO: add retries
        try (Response ignored = httpClient.execute(request)) {
        }
        catch (IOException e) {
            throw new RuntimeException("Error sending delete request to " + uri, e);
        }
    }

    // static helper methods

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

    private static boolean isJson(MediaType type)
    {
        return (type != null) && "application".equals(type.type()) && "json".equals(type.subtype());
    }

    private static QueryResults parseQueryResultsResponse(Response response)
    {
        try (ResponseBody responseBody = response.body()) {
            if (response.code() != HTTP_OK) {
                throw new RuntimeException("Query results response is not OK: " + response.code());
            }
            // TODO: access denied
            if (responseBody == null) {
                throw new RuntimeException("Query results response is empty");
            }
            if (!isJson(responseBody.contentType())) {
                throw new RuntimeException("Query results response is not json: " + responseBody.contentType());
            }
            try {
                return QUERY_RESULTS_CODEC.fromJson(responseBody.bytes());
            }
            catch (IOException e) {
                throw new RuntimeException("Error parsing query results response", e);
            }
        }
    }

    private static DataResults parseDataResultsResponse(Response response)
    {
        try (ResponseBody responseBody = response.body()) {
            if (response.code() != HTTP_OK) {
                throw new RuntimeException("Data results response is not OK: " + response.code());
            }
            if (responseBody == null) {
                throw new RuntimeException("Data results response is empty");
            }
            if (!isJson(responseBody.contentType())) {
                throw new RuntimeException("Data results response is not json: " + responseBody.contentType());
            }
            try {
                return DATA_RESULTS_JSON_CODEC.fromJson(responseBody.bytes());
            }
            catch (IOException e) {
                throw new RuntimeException("Error parsing data results response", e);
            }
        }
    }
}
