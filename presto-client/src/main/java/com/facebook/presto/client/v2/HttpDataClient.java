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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.spi.StandardErrorCode.REMOTE_BUFFER_CLOSE_FAILED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public final class HttpDataClient<T>
        implements Closeable
{
    private static final Logger log = Logger.get(HttpDataClient.class);

    /**
     * For each request, the addPage method will be called zero or more times,
     * followed by either requestComplete or clientFinished (if buffer complete).  If the client is
     * closed, requestComplete or bufferFinished may never be called.
     * <p/>
     * <b>NOTE:</b> Implementations of this interface are not allowed to perform
     * blocking operations.
     */
    public interface ClientCallback<T>
    {
        boolean addPages(HttpDataClient<T> client, DataResponse<T> data);

        void requestComplete(HttpDataClient<T> client);

        void clientFinished(HttpDataClient<T> client);

        void clientFailed(HttpDataClient<T> client, Throwable cause);
    }

    private final AsyncOkHttpClient httpClient;
    private final DataSize maxResponseSize;
    // necessary for equality checks between clients
    private final URI initialLocation;
    private final ClientCallback<T> clientCallback;
    private final ScheduledExecutorService executor;
    private final Function<Response, DataResponse<T>> responseHandler;
    private final Backoff backoff;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private ListenableFuture<?> future;
    @GuardedBy("this")
    private DateTime lastUpdate = DateTime.now();
    @GuardedBy("this")
    private boolean scheduled;
    @GuardedBy("this")
    private boolean completed;
    @GuardedBy("this")
    private URI nextUri;

    private final AtomicLong rowsReceived = new AtomicLong();
    private final AtomicInteger pagesReceived = new AtomicInteger();

    private final AtomicLong rowsRejected = new AtomicLong();
    private final AtomicInteger pagesRejected = new AtomicInteger();

    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();
    private final AtomicInteger requestsFailed = new AtomicInteger();

    public HttpDataClient(
            AsyncOkHttpClient httpClient,
            DataSize maxResponseSize,
            Duration minErrorDuration,
            Duration maxErrorDuration,
            URI location,
            ClientCallback<T> clientCallback,
            ScheduledExecutorService executor,
            Function<Response, DataResponse<T>> responseHandler)
    {
        this(httpClient, maxResponseSize, minErrorDuration, maxErrorDuration, location, clientCallback, executor, responseHandler, Ticker.systemTicker());
    }

    public HttpDataClient(
            AsyncOkHttpClient httpClient,
            DataSize maxResponseSize,
            Duration minErrorDuration,
            Duration maxErrorDuration,
            URI location,
            ClientCallback<T> clientCallback,
            ScheduledExecutorService executor,
            Function<Response, DataResponse<T>> responseHandler,
            Ticker ticker)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
        this.nextUri = this.initialLocation = requireNonNull(location, "location is null");
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.responseHandler = requireNonNull(responseHandler, "responseHandler is null");
        requireNonNull(minErrorDuration, "minErrorDuration is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(ticker, "ticker is null");
        this.backoff = new Backoff(
                minErrorDuration,
                maxErrorDuration,
                ticker,
                new Duration(0, MILLISECONDS),
                new Duration(50, MILLISECONDS),
                new Duration(100, MILLISECONDS),
                new Duration(200, MILLISECONDS),
                new Duration(500, MILLISECONDS));
    }

    /*
    public synchronized PageBufferClientStatus getStatus()
    {
        String state;
        if (closed) {
            state = "closed";
        }
        else if (future != null) {
            state = "running";
        }
        else if (scheduled) {
            state = "scheduled";
        }
        else if (completed) {
            state = "completed";
        }
        else {
            state = "queued";
        }
        String httpRequestState = "not scheduled";
        if (future != null) {
            httpRequestState = future.getState();
        }

        long rejectedRows = rowsRejected.get();
        int rejectedPages = pagesRejected.get();

        return new PageBufferClientStatus(
                location,
                state,
                lastUpdate,
                rowsReceived.get(),
                pagesReceived.get(),
                rejectedRows == 0 ? OptionalLong.empty() : OptionalLong.of(rejectedRows),
                rejectedPages == 0 ? OptionalInt.empty() : OptionalInt.of(rejectedPages),
                requestsScheduled.get(),
                requestsCompleted.get(),
                requestsFailed.get(),
                httpRequestState);
    }
    */
    public synchronized boolean isRunning()
    {
        return future != null;
    }

    @Override
    public void close()
    {
        boolean shouldSendDelete;
        Future<?> future;
        synchronized (this) {
            shouldSendDelete = !closed;

            closed = true;

            future = this.future;

            this.future = null;

            lastUpdate = DateTime.now();
        }

        if (future != null && !future.isDone()) {
            future.cancel(true);
        }

        // abort the output buffer on the remote node; response of delete is ignored
        if (shouldSendDelete) {
            sendDelete();
        }
    }

    public synchronized void scheduleRequest()
    {
        if (closed || (future != null) || scheduled) {
            return;
        }
        scheduled = true;

        // start before scheduling to include error delay
        backoff.startRequest();

        long delayNanos = backoff.getBackoffDelayNanos();
        executor.schedule(() -> {
            try {
                initiateRequest();
            }
            catch (Throwable t) {
                // should not happen, but be safe and fail the operator
                clientCallback.clientFailed(HttpDataClient.this, t);
            }
        }, delayNanos, NANOSECONDS);

        lastUpdate = DateTime.now();
        requestsScheduled.incrementAndGet();
    }

    private synchronized void initiateRequest()
    {
        scheduled = false;
        if (closed || (future != null)) {
            return;
        }

        if (completed) {
            sendDelete();
        }
        else {
            sendGetResults();
        }

        lastUpdate = DateTime.now();
    }

    private synchronized void sendGetResults()
    {
        HttpUrl uri = HttpUrl.get(nextUri).newBuilder().addQueryParameter("maxSize", maxResponseSize.toString()).build();
        ListenableFuture<Response> responseFuture = httpClient.executeAsync(
                new Request.Builder()
                        .get()
                        // TODO: configure 'accepts' per class
                        .header(ACCEPT, "application/json")
                        .url(uri)
                        .build());

        ListenableFuture<DataResponse<T>> resultFuture = Futures.transform(responseFuture, responseHandler::apply);

        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<DataResponse<T>>()
        {
            @Override
            public void onSuccess(DataResponse<T> result)
            {
                checkNotHoldsLock();

                backoff.success();

                synchronized (HttpDataClient.this) {
                    nextUri = result.getNextUri();
                }

                // add pages:
                // addPages must be called regardless of whether pages is an empty list because
                // clientCallback can keep stats of requests and responses. For example, it may
                // keep track of how often a client returns empty response and adjust request
                // frequency or buffer size.
                clientCallback.addPages(HttpDataClient.this, result);
//                if (clientCallback.addPages(HttpDataClient.this, result)) {
//                    pagesReceived.addAndGet(pages.size());
//                    rowsReceived.addAndGet(pages.stream().mapToLong(SerializedPage::getPositionCount).sum());
//                }
//                else {
//                    pagesRejected.addAndGet(pages.size());
//                    rowsRejected.addAndGet(pages.stream().mapToLong(SerializedPage::getPositionCount).sum());
//                }

                synchronized (HttpDataClient.this) {
                    // client is complete, acknowledge it by sending it a delete in the next request
                    if (result.getNextUri() == null) {
                        completed = true;
                    }
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                requestsCompleted.incrementAndGet();
                clientCallback.requestComplete(HttpDataClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                log.debug("Request to %s failed %s", uri, t);
                checkNotHoldsLock();

                if (!(t instanceof PrestoException) && backoff.failure()) {
                    String message = format("%s (%s - %s failures, time since last success %s)",
                            "WORKER_NODE_ERROR",
                            uri,
                            backoff.getFailureCount(),
                            backoff.getTimeSinceLastSuccess().convertTo(SECONDS));
                    t = new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, uri + message, t);
                }
                handleFailure(t, resultFuture);
            }
        }, executor);
    }

    private synchronized void sendDelete()
    {
        HttpUrl uri = HttpUrl.get(initialLocation);
        ListenableFuture<Response> resultFuture = httpClient.executeAsync(new Request.Builder()
                .delete()
                .url(uri)
                .build());
        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<Response>()
        {
            @Override
            public void onSuccess(@Nullable Response result)
            {
                checkNotHoldsLock();
                backoff.success();
                synchronized (HttpDataClient.this) {
                    closed = true;
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                requestsCompleted.incrementAndGet();
                clientCallback.clientFinished(HttpDataClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                checkNotHoldsLock();

                log.error("Request to delete %s failed %s", uri, t);
                if (!(t instanceof PrestoException) && backoff.failure()) {
                    String message = format("Error closing remote buffer (%s - %s failures, time since last success %s)",
                            uri,
                            backoff.getFailureCount(),
                            backoff.getTimeSinceLastSuccess().convertTo(SECONDS));
                    t = new PrestoException(REMOTE_BUFFER_CLOSE_FAILED, message, t);
                }
                handleFailure(t, resultFuture);
            }
        }, executor);
    }

    private void checkNotHoldsLock()
    {
        if (Thread.holdsLock(HttpDataClient.this)) {
            log.error("Can not handle callback while holding a lock on this");
        }
    }

    private void handleFailure(Throwable t, ListenableFuture<?> expectedFuture)
    {
        // Can not delegate to other callback while holding a lock on this
        checkNotHoldsLock();

        requestsFailed.incrementAndGet();
        requestsCompleted.incrementAndGet();

        if (t instanceof PrestoException) {
            clientCallback.clientFailed(HttpDataClient.this, t);
        }

        synchronized (HttpDataClient.this) {
            if (future == expectedFuture) {
                future = null;
            }
            lastUpdate = DateTime.now();
        }
        clientCallback.requestComplete(HttpDataClient.this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HttpDataClient that = (HttpDataClient) o;

        return Objects.equals(initialLocation, that.initialLocation);
    }

    @Override
    public int hashCode()
    {
        return initialLocation.hashCode();
    }

    @Override
    public String toString()
    {
        String state;
        synchronized (this) {
            if (closed) {
                state = "CLOSED";
            }
            else if (future != null) {
                state = "RUNNING";
            }
            else {
                state = "QUEUED";
            }
        }
        return toStringHelper(this)
                .add("initialLocation", initialLocation)
                .addValue(state)
                .toString();
    }
}
