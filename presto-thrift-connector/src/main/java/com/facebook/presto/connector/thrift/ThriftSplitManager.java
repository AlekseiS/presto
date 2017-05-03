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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.connector.thrift.api.PrestoThriftConnectorSession;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTableLayout;
import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.connector.thrift.ThriftColumnHandle.tupleDomainToThriftTupleDomain;
import static com.facebook.presto.connector.thrift.api.PrestoThriftHostAddress.toHostAddressList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;

public class ThriftSplitManager
        implements ConnectorSplitManager
{
    private final PrestoThriftServiceProvider clientProvider;
    private final ThriftClientSessionProperties clientSessionProperties;

    @Inject
    public ThriftSplitManager(PrestoThriftServiceProvider clientProvider, ThriftClientSessionProperties clientSessionProperties)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.clientSessionProperties = requireNonNull(clientSessionProperties, "clientSessionProperties is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        ThriftTableLayoutHandle layoutHandle = (ThriftTableLayoutHandle) layout;
        return new ThriftSplitSource(
                clientProvider.anyHostClient(),
                ThriftClientSessionProperties.toThriftSession(session, clientSessionProperties),
                new PrestoThriftTableLayout(layoutHandle.getLayoutId(), tupleDomainToThriftTupleDomain(layoutHandle.getPredicate())));
    }

    @NotThreadSafe
    private static class ThriftSplitSource
            implements ConnectorSplitSource
    {
        private final PrestoThriftService client;
        private final PrestoThriftConnectorSession session;
        private final PrestoThriftTableLayout layout;

        // the code assumes getNextBatch is called by a single thread

        private final AtomicBoolean hasMoreData;
        private final AtomicReference<byte[]> nextToken;
        private final AtomicReference<Future<?>> future;

        public ThriftSplitSource(
                PrestoThriftService client,
                PrestoThriftConnectorSession session,
                PrestoThriftTableLayout layout)
        {
            this.client = requireNonNull(client, "client is null");
            this.session = requireNonNull(session, "session is null");
            this.layout = requireNonNull(layout, "layout is null");
            this.nextToken = new AtomicReference<>(null);
            this.hasMoreData = new AtomicBoolean(true);
            this.future = new AtomicReference<>(null);
        }

        /**
         * Returns a future with a list of splits.
         * This method is assumed to be called in a single-threaded way.
         * It can be called by multiple threads, but only if the previous call finished.
         */
        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
        {
            checkState(future.get() == null || future.get().isDone(), "previous batch not completed");
            checkState(hasMoreData.get(), "this method cannot be invoked when there's no more data");
            byte[] currentToken = nextToken.get();
            ListenableFuture<PrestoThriftSplitBatch> splitsFuture = client.getSplits(session, layout, maxSize, currentToken);
            ListenableFuture<List<ConnectorSplit>> resultFuture = Futures.transform(
                    splitsFuture,
                    batch -> {
                        requireNonNull(batch, "batch is null");
                        List<ConnectorSplit> splits = batch.getSplits()
                                .stream()
                                .map(ThriftSplitSource::toConnectorSplit)
                                .collect(toImmutableList());
                        checkState(nextToken.compareAndSet(currentToken, batch.getNextToken()));
                        checkState(hasMoreData.compareAndSet(true, nextToken.get() != null));
                        return splits;
                    });
            future.set(resultFuture);
            return toCompletableFuture(resultFuture);
        }

        @Override
        public boolean isFinished()
        {
            return !hasMoreData.get();
        }

        @Override
        public void close()
        {
            Future<?> currentFuture = future.getAndSet(null);
            if (currentFuture != null) {
                currentFuture.cancel(true);
            }
            client.close();
        }

        private static ThriftConnectorSplit toConnectorSplit(PrestoThriftSplit thriftSplit)
        {
            return new ThriftConnectorSplit(
                    thriftSplit.getSplitId(),
                    toHostAddressList(thriftSplit.getHosts()));
        }
    }
}
