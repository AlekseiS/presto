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
package com.facebook.presto.thrift;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.thrift.clientproviders.PrestoClientProvider;
import com.facebook.presto.thrift.interfaces.client.ThriftColumnData;
import com.facebook.presto.thrift.interfaces.client.ThriftRowsBatch;
import com.facebook.presto.thrift.interfaces.writers.ColumnWriter;
import com.facebook.presto.thrift.interfaces.writers.ColumnWriters;
import com.facebook.presto.thrift.pagesources.ThriftIndexPageSource;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ThriftConnectorIndex
        implements ConnectorIndex
{
    private final PrestoClientProvider clientProvider;
    private final ThriftConfig config;
    private final byte[] indexId;
    private final List<String> inputColumnNames;
    private final List<ColumnHandle> outputColumns;

    public ThriftConnectorIndex(
            PrestoClientProvider clientProvider,
            ThriftConfig config,
            ThriftIndexHandle indexHandle,
            List<ColumnHandle> lookupColumns,
            List<ColumnHandle> outputColumns)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.config = requireNonNull(config, "config is null");
        this.indexId = requireNonNull(indexHandle, "indexHandle is null").getIndexId();
        this.inputColumnNames = requireNonNull(lookupColumns, "lookupColumns is null").stream()
                .map(handle -> ((ThriftColumnHandle) handle).getColumnName())
                .collect(toList());
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
    }

    @Override
    public ConnectorPageSource lookup(RecordSet recordSet)
    {
        ThriftRowsBatch keys = convertKeys(recordSet, inputColumnNames);
        return new ThriftIndexPageSource(clientProvider, indexId, keys, outputColumns, config);
    }

    private static ThriftRowsBatch convertKeys(RecordSet recordSet, List<String> columnNames)
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
