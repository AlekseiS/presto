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

import com.facebook.presto.connector.thrift.ThriftColumnHandle;
import com.facebook.presto.connector.thrift.ThriftConnectorConfig;
import com.facebook.presto.connector.thrift.ThriftConnectorSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftRowsBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.connector.thrift.readers.ColumnReaders.convertToPage;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ThriftPageSource
        implements ConnectorPageSource
{
    private final byte[] splitId;
    private final PrestoThriftService client;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final long maxBytesPerResponse;
    private final AtomicLong readTimeNanos = new AtomicLong(0);

    private byte[] nextToken;
    private boolean firstCall = true;
    private CompletableFuture<PrestoThriftRowsBatch> future;
    private long completedBytes;

    public ThriftPageSource(
            PrestoThriftServiceProvider clientProvider,
            ThriftConnectorConfig config,
            ThriftConnectorSplit split,
            List<ColumnHandle> columns)
    {
        // init columns
        requireNonNull(columns, "columns is null");
        List<String> columnNames = new ArrayList<>(columns.size());
        List<Type> columnTypes = new ArrayList<>(columns.size());
        for (ColumnHandle columnHandle : columns) {
            ThriftColumnHandle thriftColumnHandle = (ThriftColumnHandle) columnHandle;
            columnNames.add(thriftColumnHandle.getColumnName());
            columnTypes.add(thriftColumnHandle.getColumnType());
        }
        this.columnNames = unmodifiableList(columnNames);
        this.columnTypes = unmodifiableList(columnTypes);

        // init config variables
        this.maxBytesPerResponse = requireNonNull(config, "config is null").getMaxResponseSize().toBytes();

        // init split
        requireNonNull(split, "split is null");
        this.splitId = split.getSplitId();

        // init client
        requireNonNull(clientProvider, "clientProvider is null");
        if (split.getAddresses().isEmpty()) {
            this.client = clientProvider.anyHostClient();
        }
        else {
            this.client = clientProvider.selectedHostClient(split.getAddresses());
        }
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return !firstCall && !canGetMoreData(nextToken);
    }

    @Override
    public Page getNextPage()
    {
        if (future != null) {
            if (!future.isDone()) {
                // data request is in progress
                return null;
            }
            else {
                // response for data request is ready
                PrestoThriftRowsBatch rowsBatch = getFutureValue(future);
                Page result = processBatch(rowsBatch);
                // immediately try sending a new request
                if (canGetMoreData(nextToken)) {
                    future = sendDataRequestInternal();
                }
                else {
                    future = null;
                }
                return result;
            }
        }
        else {
            // no data request in progress
            if (firstCall || canGetMoreData(nextToken)) {
                // no data in the current batch, but can request more; will send a request
                future = sendDataRequestInternal();
            }
            return null;
        }
    }

    private boolean canGetMoreData(byte[] nextToken)
    {
        return nextToken != null;
    }

    private CompletableFuture<PrestoThriftRowsBatch> sendDataRequestInternal()
    {
        final long start = System.nanoTime();
        ListenableFuture<PrestoThriftRowsBatch> rowsBatchFuture = client.getRows(splitId, columnNames, maxBytesPerResponse, nextToken);
        rowsBatchFuture.addListener(() -> readTimeNanos.addAndGet(System.nanoTime() - start), directExecutor());
        return toCompletableFuture(rowsBatchFuture);
    }

    private Page processBatch(PrestoThriftRowsBatch rowsBatch)
    {
        firstCall = false;
        nextToken = rowsBatch.getNextToken();
        completedBytes += rowsBatch.getDataSize();
        return convertToPage(rowsBatch, columnNames, columnTypes);
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return future == null || future.isDone() ? NOT_BLOCKED : future;
    }

    @Override
    public void close()
            throws IOException
    {
        if (future != null) {
            future.cancel(true);
        }
        client.close();
    }
}
