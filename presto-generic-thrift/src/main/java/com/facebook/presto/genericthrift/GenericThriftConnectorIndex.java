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
import com.facebook.presto.genericthrift.client.ThriftSplitsOrRows;
import com.facebook.presto.genericthrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.genericthrift.writers.ColumnWriter;
import com.facebook.presto.genericthrift.writers.ColumnWriters;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class GenericThriftConnectorIndex
        implements ConnectorIndex
{
    private static final int MAX_SPLIT_COUNT = 2048;
    private static final int MAX_ROW_COUNT = 8196;
    private final PrestoClientProvider clientProvider;
    private final byte[] indexId;
    private final List<String> inputColumnNames;
    private final List<ColumnHandle> outputColumns;

    public GenericThriftConnectorIndex(PrestoClientProvider clientProvider, GenericThriftIndexHandle indexHandle, List<ColumnHandle> lookupColumns, List<ColumnHandle> outputColumns)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.indexId = requireNonNull(indexHandle, "indexHandle is null").getIndexId();
        this.inputColumnNames = requireNonNull(lookupColumns, "lookupColumns is null").stream()
                .map(handle -> ((GenericThriftColumnHandle) handle).getColumnName())
                .collect(toList());
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
    }

    @Override
    public ConnectorPageSource lookup(RecordSet recordSet)
    {
        ThriftRowsBatch keys = convertKeys(recordSet, inputColumnNames);
        ThriftPrestoClient client = clientProvider.connectToAnyHost();
        ThriftSplitsOrRows result = client.getRowsOrSplitsForIndex(indexId, keys, MAX_SPLIT_COUNT, MAX_ROW_COUNT);
        throw new UnsupportedOperationException("not implemented yet");
//        if (result.getRows() != null) {
//            return new GenericThriftContinuedIndexPageSource(client, result.getRows(), outputColumns, indexId, keys, MAX_ROW_COUNT);
//        }
//        else if (result.getSplits() != null) {
//            return new GenericThriftSplitBasedIndexPageSource(client, result.getSplits(), outputColumns, indexId, keys, MAX_SPLIT_COUNT);
//        }
//        else {
//            throw new IllegalStateException("Unknown state of splits or rows data structure");
//        }
    }

    private ThriftRowsBatch convertKeys(RecordSet recordSet, List<String> columnNames)
    {
        List<Type> columnTypes = recordSet.getColumnTypes();
        int nColumns = columnTypes.size();
        checkArgument(nColumns == columnNames.size(), "size of column types and column names doesn't match");
        List<ColumnWriter> columnWriters = new ArrayList<>(nColumns);
        int totalRecords = 0;
        try (RecordCursor cursor = recordSet.cursor()) {
            // assumes lists are indexable
            for (int i = 0; i < nColumns; i++) {
                String columName = columnNames.get(i);
                Type columnType = columnTypes.get(i);
                columnWriters.add(ColumnWriters.create(columName, columnType));
            }
            while (cursor.advanceNextPosition()) {
                for (int i = 0; i < nColumns; i++) {
                    columnWriters.get(i).append(cursor, i);
                }
                totalRecords++;
            }
        }
        List<ThriftColumnData> columnsData = new ArrayList<>(nColumns);
        for (ColumnWriter writer : columnWriters) {
            columnsData.addAll(writer.getResult());
        }
        return new ThriftRowsBatch(columnsData, totalRecords, null);
    }
}
