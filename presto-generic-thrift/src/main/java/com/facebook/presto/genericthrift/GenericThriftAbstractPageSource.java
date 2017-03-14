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

import com.facebook.presto.genericthrift.client.ThriftColumnData;
import com.facebook.presto.genericthrift.client.ThriftRowsBatch;
import com.facebook.presto.genericthrift.readers.ColumnReader;
import com.facebook.presto.genericthrift.readers.ColumnReaders;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;

public abstract class GenericThriftAbstractPageSource
        implements ConnectorPageSource
{
    private static final int MAX_RECORDS_PER_REQUEST = 8192;
    private static final int DEFAULT_NUM_RECORDS = 4096;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final int numberOfColumns;
    private final ArrayList<ColumnReader> readers;
    private byte[] nextToken;
    private boolean firstCall = true;
    private CompletableFuture<ThriftRowsBatch> future;

    public GenericThriftAbstractPageSource(List<ColumnHandle> columns)
    {
        checkArgument(columns != null && !columns.isEmpty(), "columns is null or empty");
        this.numberOfColumns = columns.size();
        this.columnNames = new ArrayList<>(numberOfColumns);
        this.columnTypes = new ArrayList<>(numberOfColumns);
        for (ColumnHandle columnHandle : columns) {
            GenericThriftColumnHandle thriftColumnHandle = (GenericThriftColumnHandle) columnHandle;
            columnNames.add(thriftColumnHandle.getColumnName());
            columnTypes.add(thriftColumnHandle.getColumnType());
        }
        readers = new ArrayList<>(Collections.nCopies(columns.size(), null));
    }

    public GenericThriftAbstractPageSource(List<ColumnHandle> columns, ThriftRowsBatch initialResponse)
    {
        this(columns);
        processResponse(initialResponse);
    }

    @Override
    public final long getTotalBytes()
    {
        return 0;
    }

    @Override
    public final long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public final long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public final boolean isFinished()
    {
        return !firstCall && !readersHaveMoreData() && nextToken == null;
    }

    private boolean readersHaveMoreData()
    {
        return readers.get(0).hasMoreRecords();
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
                processResponse(getFutureValue(future));
                return nextPageFromCurrentBatch();
            }
        }
        else {
            // no data request in progress
            if (firstCall || (!readersHaveMoreData() && nextToken != null)) {
                // no data in the current batch, but can request more; will send a request
                future = toCompletableFuture(sendRequestForData(nextToken, MAX_RECORDS_PER_REQUEST));
                return null;
            }
            else {
                // either data is available or cannot request more
                return nextPageFromCurrentBatch();
            }
        }
    }

    public abstract ListenableFuture<ThriftRowsBatch> sendRequestForData(byte[] nextToken, int maxRecords);

    public abstract void closeInternal();

    private void processResponse(ThriftRowsBatch response)
    {
        firstCall = false;
        future = null;
        nextToken = response.getNextToken();
        List<ThriftColumnData> columnsData = response.getColumnsData();
        for (int i = 0; i < numberOfColumns; i++) {
            readers.set(i, ColumnReaders.create(columnsData, columnNames.get(i), columnTypes.get(i), response.getRowCount()));
        }
        checkState(readersHaveMoreData() || nextToken == null, "Batch cannot be empty when continuation token is present");
    }

    private Page nextPageFromCurrentBatch()
    {
        if (readersHaveMoreData()) {
            Block[] blocks = new Block[numberOfColumns];
            for (int i = 0; i < numberOfColumns; i++) {
                blocks[i] = readers.get(i).readBlock(DEFAULT_NUM_RECORDS);
            }
            return new Page(blocks);
        }
        else {
            return null;
        }
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
