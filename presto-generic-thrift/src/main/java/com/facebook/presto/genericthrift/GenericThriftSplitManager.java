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
package com.facebook.presto.genericthrift;

import com.facebook.presto.genericthrift.client.ThriftConnectorSession;
import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.client.ThriftSchemaTableName;
import com.facebook.presto.genericthrift.client.ThriftSplit;
import com.facebook.presto.genericthrift.client.ThriftSplitBatch;
import com.facebook.presto.genericthrift.client.ThriftTableLayout;
import com.facebook.presto.genericthrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.genericthrift.client.ThriftConnectorSession.fromConnectorSession;
import static com.facebook.presto.genericthrift.client.ThriftSchemaTableName.fromSchemaTableName;
import static com.facebook.presto.genericthrift.client.ThriftTupleDomain.fromTupleDomain;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toList;

public class GenericThriftSplitManager
        implements ConnectorSplitManager
{
    private final PrestoClientProvider clientProvider;
    private final GenericThriftClientSessionProperties clientSessionProperties;
    private final ExecutorService executor;

    @Inject
    public GenericThriftSplitManager(PrestoClientProvider clientProvider, GenericThriftClientSessionProperties clientSessionProperties)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.clientSessionProperties = requireNonNull(clientSessionProperties, "clientSessionProperties is null");
        this.executor = newCachedThreadPool(threadsNamed("splits-transform-%s"));
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        GenericThriftTableLayoutHandle layoutHandle = (GenericThriftTableLayoutHandle) layout;
        return new GenericThriftSplitSource(
                clientProvider.connectToAnyHost(),
                fromConnectorSession(session, clientSessionProperties),
                fromSchemaTableName(layoutHandle.getSchemaTableName()),
                new ThriftTableLayout(
                        layoutHandle.getOutputColumns()
                                .map(columns -> columns
                                        .stream()
                                        .map(column -> ((GenericThriftColumnHandle) column).getColumnName())
                                        .collect(toList()))
                                .orElse(null),
                        fromTupleDomain(layoutHandle.getPredicate())),
                executor);
    }

    @SuppressWarnings("unused")
    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }

    private static class GenericThriftSplitSource
            implements ConnectorSplitSource
    {
        private final ThriftPrestoClient client;
        private final ThriftConnectorSession session;
        private final ThriftSchemaTableName schemaTableName;
        private final ThriftTableLayout layout;
        private final ExecutorService executor;

        // the code assumes getNextBatch is called by a single thread

        private final AtomicBoolean hasMoreData;
        private final AtomicReference<byte[]> continuationToken;
        private final AtomicReference<Future<?>> future;

        public GenericThriftSplitSource(
                ThriftPrestoClient client,
                ThriftConnectorSession session,
                ThriftSchemaTableName schemaTableName,
                ThriftTableLayout layout,
                ExecutorService executor)
        {
            this.client = requireNonNull(client, "client is null");
            this.session = requireNonNull(session, "session is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.layout = requireNonNull(layout, "layout is null");
            this.executor = requireNonNull(executor, "executor is null");
            this.continuationToken = new AtomicReference<>(null);
            this.hasMoreData = new AtomicBoolean(true);
            this.future = new AtomicReference<>(null);
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
        {
            checkState(hasMoreData.get());
            byte[] currentContinuationToken = continuationToken.get();
            ListenableFuture<ThriftSplitBatch> splitsFuture = client.getSplitBatch(session, schemaTableName, layout, maxSize, currentContinuationToken);
            ListenableFuture<List<ConnectorSplit>> resultFuture = Futures.transform(
                    splitsFuture,
                    batch -> {
                        requireNonNull(batch, "batch is null");
                        List<ConnectorSplit> splits = batch.getSplits().stream().map(ThriftSplit::toConnectorSplit).collect(toList());
                        checkState(continuationToken.compareAndSet(currentContinuationToken, batch.getNextToken()));
                        checkState(hasMoreData.compareAndSet(true, continuationToken.get() != null));
                        return splits;
                    },
                    executor);
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
    }
}
