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
package com.facebook.presto.thrift.pagesources;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.thrift.ThriftColumnHandle;
import com.facebook.presto.thrift.ThriftConnectorConfig;
import com.facebook.presto.thrift.interfaces.client.ThriftRowsBatch;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.thrift.interfaces.readers.ColumnReaders.convertToPage;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;

public abstract class AbstractThriftPageSource
        implements ConnectorPageSource
{
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final long maxBytesPerResponse;
    private byte[] nextToken;
    private boolean firstCall = true;
    private CompletableFuture<ThriftRowsBatch> future;
    private final AtomicLong readTimeNanos = new AtomicLong(0);
    private long completedBytes;

    public AbstractThriftPageSource(List<ColumnHandle> columns, ThriftConnectorConfig config)
    {
        requireNonNull(columns, "columns is null");
        this.columnNames = new ArrayList<>(columns.size());
        this.columnTypes = new ArrayList<>(columns.size());
        for (ColumnHandle columnHandle : columns) {
            ThriftColumnHandle thriftColumnHandle = (ThriftColumnHandle) columnHandle;
            columnNames.add(thriftColumnHandle.getColumnName());
            columnTypes.add(thriftColumnHandle.getColumnType());
        }
        this.maxBytesPerResponse = requireNonNull(config, "config is null").getMaxResponseSize().toBytes();
    }

    public abstract ListenableFuture<ThriftRowsBatch> sendDataRequest(byte[] nextToken, long maxBytes);

    public abstract void closeInternal();

    public boolean canGetMoreData(byte[] nextToken)
    {
        return nextToken != null;
    }

    @Override
    public final long getTotalBytes()
    {
        return 0;
    }

    @Override
    public final long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public final long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public final boolean isFinished()
    {
        return !firstCall && !canGetMoreData(nextToken);
    }

    @Override
    public final Page getNextPage()
    {
        if (future != null) {
            if (!future.isDone()) {
                // data request is in progress
                return null;
            }
            else {
                // response for data request is ready
                ThriftRowsBatch rowsBatch = getFutureValue(future);
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

    private CompletableFuture<ThriftRowsBatch> sendDataRequestInternal()
    {
        final long start = System.nanoTime();
        ListenableFuture<ThriftRowsBatch> rowsBatchFuture = sendDataRequest(nextToken, maxBytesPerResponse);
        rowsBatchFuture.addListener(() -> readTimeNanos.addAndGet(System.nanoTime() - start), directExecutor());
        return toCompletableFuture(rowsBatchFuture);
    }

    private Page processBatch(ThriftRowsBatch rowsBatch)
    {
        firstCall = false;
        nextToken = rowsBatch.getNextToken();
        completedBytes += rowsBatch.getDataSize();
        return convertToPage(rowsBatch, columnNames, columnTypes);
    }

    @Override
    public final CompletableFuture<?> isBlocked()
    {
        return future == null || future.isDone() ? NOT_BLOCKED : future;
    }

    @Override
    public final long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public final void close()
            throws IOException
    {
        if (future != null) {
            future.cancel(true);
        }
        closeInternal();
    }
}
