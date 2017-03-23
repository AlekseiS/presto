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
package com.facebook.presto.thrift;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.thrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.thrift.interfaces.client.ThriftConnectorSession;
import com.facebook.presto.thrift.interfaces.client.ThriftPrestoClient;
import com.facebook.presto.thrift.interfaces.client.ThriftSplit;
import com.facebook.presto.thrift.interfaces.client.ThriftSplitBatch;
import com.facebook.presto.thrift.interfaces.client.ThriftTableLayout;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.thrift.ThriftColumnHandle.tupleDomainToThriftTupleDomain;
import static com.facebook.presto.thrift.interfaces.client.ThriftHostAddress.toHostAddressList;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ThriftSplitManager
        implements ConnectorSplitManager
{
    private final PrestoClientProvider clientProvider;
    private final ThriftClientSessionProperties clientSessionProperties;

    @Inject
    public ThriftSplitManager(PrestoClientProvider clientProvider, ThriftClientSessionProperties clientSessionProperties)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.clientSessionProperties = requireNonNull(clientSessionProperties, "clientSessionProperties is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        ThriftTableLayoutHandle layoutHandle = (ThriftTableLayoutHandle) layout;
        return new ThriftSplitSource(
                clientProvider.connectToAnyHost(),
                ThriftClientSessionProperties.toThriftSession(session, clientSessionProperties),
                new ThriftTableLayout(layoutHandle.getLayoutId(), tupleDomainToThriftTupleDomain(layoutHandle.getPredicate())));
    }

    private static class ThriftSplitSource
            implements ConnectorSplitSource
    {
        private final ThriftPrestoClient client;
        private final ThriftConnectorSession session;
        private final ThriftTableLayout layout;

        // the code assumes getNextBatch is called by a single thread

        private final AtomicBoolean hasMoreData;
        private final AtomicReference<byte[]> continuationToken;
        private final AtomicReference<Future<?>> future;

        public ThriftSplitSource(
                ThriftPrestoClient client,
                ThriftConnectorSession session,
                ThriftTableLayout layout)
        {
            this.client = requireNonNull(client, "client is null");
            this.session = requireNonNull(session, "session is null");
            this.layout = requireNonNull(layout, "layout is null");
            this.continuationToken = new AtomicReference<>(null);
            this.hasMoreData = new AtomicBoolean(true);
            this.future = new AtomicReference<>(null);
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
        {
            checkState(future.get() == null || future.get().isDone(), "previous batch not completed");
            checkState(hasMoreData.get(), "this method should not be called when there's no more data");
            byte[] currentContinuationToken = continuationToken.get();
            ListenableFuture<ThriftSplitBatch> splitsFuture = client.getSplits(session, layout, maxSize, currentContinuationToken);
            ListenableFuture<List<ConnectorSplit>> resultFuture = Futures.transform(
                    splitsFuture,
                    batch -> {
                        requireNonNull(batch, "batch is null");
                        List<ConnectorSplit> splits = batch.getSplits().stream()
                                .map(ThriftSplitSource::thriftSplitToConnectorSplit)
                                .collect(toList());
                        checkState(continuationToken.compareAndSet(currentContinuationToken, batch.getNextToken()));
                        checkState(hasMoreData.compareAndSet(true, continuationToken.get() != null));
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

        private static ThriftConnectorSplit thriftSplitToConnectorSplit(ThriftSplit thriftSplit)
        {
            return new ThriftConnectorSplit(
                    thriftSplit.getSplitId(),
                    toHostAddressList(thriftSplit.getHosts()));
        }
    }
}
