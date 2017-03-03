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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.genericthrift.client.ThriftConnectorSession.fromConnectorSession;
import static com.facebook.presto.genericthrift.client.ThriftSchemaTableName.fromSchemaTableName;
import static com.facebook.presto.genericthrift.client.ThriftTupleDomain.fromTupleDomain;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
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
        this.executor = newCachedThreadPool(threadsNamed("splits-fetcher-%s"));
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
        private final AtomicReference<String> continuationToken;
        private final AtomicReference<CompletableFuture<List<ConnectorSplit>>> future;

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
            CompletableFuture<List<ConnectorSplit>> newFuture = supplyAsync(() -> {
                checkState(hasMoreData.get());
                String currentContinuationToken = continuationToken.get();
                ThriftSplitBatch batch = client.getSplitBatch(session, schemaTableName, layout, maxSize, currentContinuationToken);
                List<ConnectorSplit> splits = batch.getSplits().stream().map(ThriftSplit::toConnectorSplit).collect(toList());
                checkState(continuationToken.compareAndSet(currentContinuationToken, batch.getNextToken()));
                checkState(hasMoreData.compareAndSet(true, continuationToken.get() != null));
                return splits;
            }, executor);
            future.set(newFuture);
            return newFuture;
        }

        @Override
        public boolean isFinished()
        {
            return !hasMoreData.get();
        }

        @Override
        public void close()
        {
            CompletableFuture<?> currentFuture = future.getAndSet(null);
            if (currentFuture != null) {
                currentFuture.cancel(true);
            }
            client.close();
        }
    }
}
