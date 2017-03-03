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
import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.client.ThriftRowsBatch;
import com.facebook.presto.genericthrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.genericthrift.readers.ColumnReader;
import com.facebook.presto.genericthrift.readers.ColumnReaders;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class GenericThriftPageSource
        implements ConnectorPageSource
{
    private static final int MAX_RECORDS_PER_REQUEST = 8192;
    private static final int DEFAULT_NUM_RECORDS = 4096;

    private final ThriftPrestoClient client;
    private final GenericThriftSplit split;
    private final List<GenericThriftColumnHandle> columns;
    private final List<String> columnNames;

    private final ArrayList<ColumnReader> readers;
    private String nextToken;
    private boolean firstCall = true;

    public GenericThriftPageSource(
            PrestoClientProvider clientProvider,
            GenericThriftSplit split,
            List<GenericThriftColumnHandle> columns)
    {
        this.split = requireNonNull(split, "split is null");
        checkArgument(columns != null && !columns.isEmpty(), "columns is null or empty");
        this.columns = ImmutableList.copyOf(columns);
        this.columnNames = columns.stream().map(GenericThriftColumnHandle::getColumnName).collect(toList());
        requireNonNull(clientProvider, "clientProvider is null");
        if (split.getAddresses().isEmpty()) {
            this.client = clientProvider.connectToAnyHost();
        }
        else {
            this.client = clientProvider.connectToAnyOf(split.getAddresses());
        }
        readers = new ArrayList<>(Collections.nCopies(columns.size(), null));
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return !firstCall && !readersHaveMoreData() && nextToken == null;
    }

    private boolean readersHaveMoreData()
    {
        return readers.get(0).hasMoreRecords();
    }

    @Override
    public Page getNextPage()
    {
        if (firstCall || (!readersHaveMoreData() && nextToken != null)) {
            ThriftRowsBatch response = client.getRows(split.getSplitId(), columnNames, MAX_RECORDS_PER_REQUEST, nextToken);
            firstCall = false;
            nextToken = response.getNextToken();
            List<ThriftColumnData> columnsData = response.getColumnsData();
            for (int i = 0; i < columns.size(); i++) {
                GenericThriftColumnHandle column = columns.get(i);
                readers.set(i, ColumnReaders.createColumnReader(columnsData, column.getColumnName(), column.getColumnType(), response.getRowCount()));
            }
            checkState(readersHaveMoreData() || nextToken == null, "Batch cannot be empty when continuation token is present");
        }
        if (readersHaveMoreData()) {
            Block[] blocks = new Block[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                blocks[i] = readers.get(i).readBlock(DEFAULT_NUM_RECORDS);
            }
            return new Page(blocks);
        }
        else {
            return null;
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        client.close();
    }
}
