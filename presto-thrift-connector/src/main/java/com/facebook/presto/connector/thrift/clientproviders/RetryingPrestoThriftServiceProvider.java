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
package com.facebook.presto.connector.thrift.clientproviders;

import com.facebook.presto.connector.thrift.annotations.NonRetrying;
import com.facebook.presto.connector.thrift.api.PrestoThriftConnectorSession;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableIndexLayoutResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableSchemaName;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftPropertyMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftRowsBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftServiceException;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitsOrRows;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.connector.thrift.util.RetryDriver;
import com.facebook.presto.spi.HostAddress;
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

public class RetryingPrestoThriftServiceProvider
        implements PrestoThriftServiceProvider
{
    private static final Logger log = Logger.get(RetryingPrestoThriftServiceProvider.class);
    private final PrestoThriftServiceProvider original;
    private final RetryDriver retry;

    @Inject
    public RetryingPrestoThriftServiceProvider(@NonRetrying PrestoThriftServiceProvider original)
    {
        this.original = requireNonNull(original, "original is null");
        retry = RetryDriver.retry()
                .maxAttempts(5)
                .stopRetryingWhen(e -> e instanceof PrestoThriftServiceException && !((PrestoThriftServiceException) e).isRetryable())
                .exponentialBackoff(
                        new Duration(10, TimeUnit.MILLISECONDS),
                        new Duration(20, TimeUnit.MILLISECONDS),
                        new Duration(30, TimeUnit.SECONDS),
                        1.5);
    }

    @Override
    public PrestoThriftService anyHostClient()
    {
        return new RetryingService(original::anyHostClient, retry);
    }

    @Override
    public PrestoThriftService selectedHostClient(List<HostAddress> hosts)
    {
        return new RetryingService(() -> original.selectedHostClient(hosts), retry);
    }

    @NotThreadSafe
    private static final class RetryingService
            implements PrestoThriftService
    {
        private final Supplier<PrestoThriftService> clientSupplier;
        private final RetryDriver retry;
        private PrestoThriftService client;

        public RetryingService(Supplier<PrestoThriftService> clientSupplier, RetryDriver retry)
        {
            this.clientSupplier = requireNonNull(clientSupplier, "clientSupplier is null");
            this.retry = retry.onRetry(this::close);
        }

        private PrestoThriftService getClient()
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
        public List<PrestoThriftPropertyMetadata> listSessionProperties()
        {
            return retry.run("listSessionProperties", () -> getClient().listSessionProperties());
        }

        @Override
        public List<String> listSchemaNames()
        {
            return retry.run("listSchemaNames", () -> getClient().listSchemaNames());
        }

        @Override
        public List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
        {
            return retry.run("listTables", () -> getClient().listTables(schemaNameOrNull));
        }

        @Override
        public PrestoThriftNullableTableMetadata getTableMetadata(PrestoThriftSchemaTableName schemaTableName)
        {
            return retry.run("getTableMetadata", () -> getClient().getTableMetadata(schemaTableName));
        }

        @Override
        public ListenableFuture<PrestoThriftSplitBatch> getSplits(
                PrestoThriftConnectorSession session,
                PrestoThriftSchemaTableName schemaTableName,
                @Nullable Set<String> desiredColumns,
                PrestoThriftTupleDomain outputConstraint,
                int maxSplitCount,
                @Nullable byte[] nextToken)
                throws PrestoThriftServiceException
        {
            return retry.run("getSplits", () -> getClient().getSplits(session, schemaTableName, desiredColumns, outputConstraint, maxSplitCount, nextToken));
        }

        @Override
        public ListenableFuture<PrestoThriftRowsBatch> getRows(byte[] splitId, List<String> columns, long maxBytes, @Nullable byte[] nextToken)
        {
            return retry.run("getRows", () -> getClient().getRows(splitId, columns, maxBytes, nextToken));
        }

        @Override
        public PrestoThriftNullableIndexLayoutResult resolveIndex(
                PrestoThriftConnectorSession session,
                PrestoThriftSchemaTableName schemaTableName,
                Set<String> indexableColumnNames,
                Set<String> outputColumnNames,
                PrestoThriftTupleDomain outputConstraint)
        {
            return retry.run("resolveIndex", () -> getClient().resolveIndex(session, schemaTableName, indexableColumnNames, outputColumnNames, outputConstraint));
        }

        @Override
        public ListenableFuture<PrestoThriftSplitsOrRows> getRowsOrSplitsForIndex(
                byte[] indexId,
                PrestoThriftRowsBatch keys,
                int maxSplitCount,
                long rowsMaxBytes,
                @Nullable byte[] nextToken)
        {
            return retry.run("getRowsOrSplitsForIndex", () -> getClient().getRowsOrSplitsForIndex(indexId, keys, maxSplitCount, rowsMaxBytes, nextToken));
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
