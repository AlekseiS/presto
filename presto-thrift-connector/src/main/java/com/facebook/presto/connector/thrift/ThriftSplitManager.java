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

import com.facebook.presto.connector.thrift.api.PrestoThriftDomain;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableColumnSet;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableToken;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.connector.thrift.api.PrestoThriftDomain.fromDomain;
import static com.facebook.presto.connector.thrift.api.PrestoThriftHostAddress.toHostAddressList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;

public class ThriftSplitManager
        implements ConnectorSplitManager
{
    private final PrestoThriftServiceProvider clientProvider;

    @Inject
    public ThriftSplitManager(PrestoThriftServiceProvider clientProvider)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        ThriftTableLayoutHandle layoutHandle = (ThriftTableLayoutHandle) layout;
        return new ThriftSplitSource(
                clientProvider.anyHostClient(),
                new PrestoThriftSchemaTableName(layoutHandle.getSchemaName(), layoutHandle.getTableName()),
                layoutHandle.getColumns().map(ThriftSplitManager::columnNames),
                tupleDomainToThriftTupleDomain(layoutHandle.getConstraint()));
    }

    private static Set<String> columnNames(Set<ColumnHandle> columns)
    {
        return columns.stream()
                .map(ThriftColumnHandle.class::cast)
                .map(ThriftColumnHandle::getColumnName)
                .collect(toImmutableSet());
    }

    private static PrestoThriftTupleDomain tupleDomainToThriftTupleDomain(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (!tupleDomain.getDomains().isPresent()) {
            return new PrestoThriftTupleDomain(null);
        }
        Map<String, PrestoThriftDomain> thriftDomains = tupleDomain.getDomains().get()
                .entrySet()
                .stream()
                .collect(toImmutableMap(
                        kv -> ((ThriftColumnHandle) kv.getKey()).getColumnName(),
                        kv -> fromDomain(kv.getValue())));
        return new PrestoThriftTupleDomain(thriftDomains);
    }

    @NotThreadSafe
    private static class ThriftSplitSource
            implements ConnectorSplitSource
    {
        private final PrestoThriftService client;

        // the code assumes getNextBatch is called by a single thread

        private final AtomicBoolean hasMoreData;
        private final AtomicReference<byte[]> nextToken;
        private final AtomicReference<Future<?>> future;
        private final PrestoThriftSchemaTableName schemaTableName;
        private final Optional<Set<String>> columnNames;
        private final PrestoThriftTupleDomain constraint;

        public ThriftSplitSource(
                PrestoThriftService client,
                PrestoThriftSchemaTableName schemaTableName,
                Optional<Set<String>> columnNames,
                PrestoThriftTupleDomain constraint)
        {
            this.client = requireNonNull(client, "client is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.columnNames = requireNonNull(columnNames, "columnNames is null");
            this.constraint = requireNonNull(constraint, "constraint is null");
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
            ListenableFuture<PrestoThriftSplitBatch> splitsFuture = client.getSplits(
                    schemaTableName,
                    new PrestoThriftNullableColumnSet(columnNames.orElse(null)),
                    constraint,
                    maxSize,
                    new PrestoThriftNullableToken(currentToken));
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
