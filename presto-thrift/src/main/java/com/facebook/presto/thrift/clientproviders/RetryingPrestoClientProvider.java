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
package com.facebook.presto.thrift.clientproviders;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.thrift.annotations.NonRetrying;
import com.facebook.presto.thrift.interfaces.client.ThriftConnectorSession;
import com.facebook.presto.thrift.interfaces.client.ThriftNullableIndexLayoutResult;
import com.facebook.presto.thrift.interfaces.client.ThriftNullableTableMetadata;
import com.facebook.presto.thrift.interfaces.client.ThriftPrestoClient;
import com.facebook.presto.thrift.interfaces.client.ThriftPropertyMetadata;
import com.facebook.presto.thrift.interfaces.client.ThriftRowsBatch;
import com.facebook.presto.thrift.interfaces.client.ThriftSchemaTableName;
import com.facebook.presto.thrift.interfaces.client.ThriftServiceException;
import com.facebook.presto.thrift.interfaces.client.ThriftSplitBatch;
import com.facebook.presto.thrift.interfaces.client.ThriftSplitsOrRows;
import com.facebook.presto.thrift.interfaces.client.ThriftTableLayout;
import com.facebook.presto.thrift.interfaces.client.ThriftTableLayoutResult;
import com.facebook.presto.thrift.interfaces.client.ThriftTupleDomain;
import com.facebook.presto.thrift.util.RetryDriver;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class RetryingPrestoClientProvider
        implements PrestoClientProvider
{
    private static final Logger log = Logger.get(RetryingPrestoClientProvider.class);
    private final PrestoClientProvider original;
    private final RetryDriver retry;

    @Inject
    public RetryingPrestoClientProvider(@NonRetrying PrestoClientProvider original)
    {
        this.original = requireNonNull(original, "original is null");
        retry = RetryDriver.retry()
                .maxAttempts(5)
                .stopRetryingWhen(e -> e instanceof ThriftServiceException && !((ThriftServiceException) e).isRetryPossible())
                .exponentialBackoff(
                        new Duration(10, TimeUnit.MILLISECONDS),
                        new Duration(20, TimeUnit.MILLISECONDS),
                        new Duration(30, TimeUnit.SECONDS),
                        1.5);
    }

    @Override
    public ThriftPrestoClient connectToAnyHost()
    {
        return new RetryingClient(original::connectToAnyHost, retry);
    }

    @Override
    public ThriftPrestoClient connectToAnyOf(List<HostAddress> hosts)
    {
        return new RetryingClient(() -> original.connectToAnyOf(hosts), retry);
    }

    @NotThreadSafe
    private static final class RetryingClient
            implements ThriftPrestoClient
    {
        private final Supplier<ThriftPrestoClient> clientSupplier;
        private final RetryDriver retry;
        private ThriftPrestoClient client;

        public RetryingClient(Supplier<ThriftPrestoClient> clientSupplier, RetryDriver retry)
        {
            this.clientSupplier = requireNonNull(clientSupplier, "clientSupplier is null");
            this.retry = retry.onRetry(this::close);
        }

        private ThriftPrestoClient getClient()
        {
            if (client != null) {
                return client;
            }
            else {
                client = clientSupplier.get();
                return client;
            }
        }

        @Override
        public List<ThriftPropertyMetadata> listSessionProperties()
        {
            return retry.run("listSessionProperties", () -> getClient().listSessionProperties());
        }

        @Override
        public List<String> listSchemaNames()
        {
            return retry.run("listSchemaNames", () -> getClient().listSchemaNames());
        }

        @Override
        public List<ThriftSchemaTableName> listTables(@Nullable String schemaNameOrNull)
        {
            return retry.run("listTables", () -> getClient().listTables(schemaNameOrNull));
        }

        @Override
        public ThriftNullableTableMetadata getTableMetadata(ThriftSchemaTableName schemaTableName)
        {
            return retry.run("getTableMetadata", () -> getClient().getTableMetadata(schemaTableName));
        }

        @Override
        public List<ThriftTableLayoutResult> getTableLayouts(
                ThriftConnectorSession session,
                ThriftSchemaTableName schemaTableName,
                ThriftTupleDomain outputConstraint,
                @Nullable Set<String> desiredColumns)
        {
            return retry.run("getTableLayouts",
                    () -> getClient().getTableLayouts(session, schemaTableName, outputConstraint, desiredColumns));
        }

        @Override
        public ListenableFuture<ThriftSplitBatch> getSplits(
                ThriftConnectorSession session,
                ThriftTableLayout layout,
                int maxSplitCount,
                @Nullable byte[] continuationToken)
        {
            return retry.run("getSplits",
                    () -> getClient().getSplits(session, layout, maxSplitCount, continuationToken));
        }

        @Override
        public ListenableFuture<ThriftRowsBatch> getRows(byte[] splitId, long maxBytes, @Nullable byte[] continuationToken)
        {
            return retry.run("getRows", () -> getClient().getRows(splitId, maxBytes, continuationToken));
        }

        @Override
        public ThriftNullableIndexLayoutResult resolveIndex(
                ThriftConnectorSession session,
                ThriftSchemaTableName schemaTableName,
                Set<String> indexableColumnNames,
                Set<String> outputColumnNames,
                ThriftTupleDomain outputConstraint)
        {
            return retry.run("resolveIndex",
                    () -> getClient().resolveIndex(session, schemaTableName, indexableColumnNames, outputColumnNames, outputConstraint));
        }

        @Override
        public ListenableFuture<ThriftSplitsOrRows> getRowsOrSplitsForIndex(
                byte[] indexId,
                ThriftRowsBatch keys,
                int maxSplitCount,
                long rowsMaxBytes,
                @Nullable byte[] continuationToken)
        {
            return retry.run("getRowsOrSplitsForIndex",
                    () -> getClient().getRowsOrSplitsForIndex(indexId, keys, maxSplitCount, rowsMaxBytes, continuationToken));
        }

        @Override
        public void close()
        {
            if (client == null) {
                return;
            }
            try {
                client.close();
            }
            catch (Exception e) {
                log.warn("Error closing client", e);
            }
            client = null;
        }
    }
}
