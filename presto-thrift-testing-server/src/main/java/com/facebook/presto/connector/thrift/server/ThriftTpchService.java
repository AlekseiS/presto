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
package com.facebook.presto.connector.thrift.server;

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnData;
import com.facebook.presto.connector.thrift.api.PrestoThriftColumnMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftConnectorSession;
import com.facebook.presto.connector.thrift.api.PrestoThriftIndexLayoutResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableIndexLayoutResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableSchemaName;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftPropertyMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftRowsBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftServiceException;
import com.facebook.presto.connector.thrift.api.PrestoThriftSessionValue;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitsOrRows;
import com.facebook.presto.connector.thrift.api.PrestoThriftTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.connector.thrift.server.states.IndexInfo;
import com.facebook.presto.connector.thrift.server.states.SplitInfo;
import com.facebook.presto.connector.thrift.writers.ColumnWriter;
import com.facebook.presto.connector.thrift.writers.ColumnWriters;
import com.facebook.presto.operator.index.PageRecordSet;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.MappedRecordSet;
import com.facebook.presto.tests.tpch.TpchIndexedData;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.connector.thrift.readers.ColumnReaders.convertToPage;
import static com.facebook.presto.connector.thrift.server.TpchServerUtils.computeRemap;
import static com.facebook.presto.connector.thrift.server.TpchServerUtils.estimateRecords;
import static com.facebook.presto.connector.thrift.server.TpchServerUtils.getTypeString;
import static com.facebook.presto.connector.thrift.server.TpchServerUtils.schemaNameToScaleFactor;
import static com.facebook.presto.connector.thrift.server.TpchServerUtils.types;
import static com.facebook.presto.connector.thrift.server.states.SerializationUtils.deserialize;
import static com.facebook.presto.connector.thrift.server.states.SerializationUtils.serialize;
import static com.facebook.presto.tests.AbstractTestIndexedQueries.INDEX_SPEC;
import static com.facebook.presto.tpch.TpchMetadata.ROW_NUMBER_COLUMN_NAME;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.lang.Math.min;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toList;

public class ThriftTpchService
        implements PrestoThriftService
{
    private static final int DEFAULT_NUMBER_OF_SPLITS = 3;
    private static final String NUMBER_OF_SPLITS_PARAMETER = "splits";
    private static final List<String> SCHEMAS = ImmutableList.of("tiny", "sf1");
    private static final int MAX_WRITERS_INITIAL_CAPACITY = 8192;

    private final ListeningExecutorService splitsExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("splits-generator-%s")));
    private final ListeningExecutorService dataExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("data-generator-%s")));
    private final ListeningExecutorService indexDataExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("index-data-generator-%s")));
    private final TpchIndexedData indexedData = new TpchIndexedData("tpchindexed", INDEX_SPEC);

    @Override
    public List<PrestoThriftPropertyMetadata> listSessionProperties()
    {
        return ImmutableList.of(new PrestoThriftPropertyMetadata(NUMBER_OF_SPLITS_PARAMETER, "integer", "Number of splits",
                new PrestoThriftSessionValue(false, null, 3, null, null, null),
                false));
    }

    @Override
    public List<String> listSchemaNames()
    {
        return SCHEMAS;
    }

    @Override
    public List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
    {
        List<PrestoThriftSchemaTableName> result = new ArrayList<>();
        for (String schemaName : getSchemaNames(schemaNameOrNull.getSchemaName())) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                result.add(new PrestoThriftSchemaTableName(schemaName, tpchTable.getTableName()));
            }
        }
        return result;
    }

    private static List<String> getSchemaNames(String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return SCHEMAS;
        }
        else if (SCHEMAS.contains(schemaNameOrNull)) {
            return ImmutableList.of(schemaNameOrNull);
        }
        else {
            return ImmutableList.of();
        }
    }

    @Override
    public PrestoThriftNullableTableMetadata getTableMetadata(PrestoThriftSchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        if (!SCHEMAS.contains(schemaName) || TpchTable.getTables().stream().noneMatch(table -> table.getTableName().equals(tableName))) {
            return new PrestoThriftNullableTableMetadata(null);
        }
        TpchTable<?> tpchTable = TpchTable.getTable(schemaTableName.getTableName());
        List<PrestoThriftColumnMetadata> columns = new ArrayList<>();
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new PrestoThriftColumnMetadata(column.getSimplifiedColumnName(), getTypeString(column.getType()), null, false));
        }
        columns.add(new PrestoThriftColumnMetadata(ROW_NUMBER_COLUMN_NAME, "bigint", null, true));
        return new PrestoThriftNullableTableMetadata(new PrestoThriftTableMetadata(schemaTableName, columns, null));
    }

    private static int getOrElse(Map<String, PrestoThriftSessionValue> values, String name, int defaultValue)
    {
        PrestoThriftSessionValue parameterValue = values.get(name);
        if (parameterValue != null && !parameterValue.isNullValue()) {
            return parameterValue.getIntValue();
        }
        else {
            return defaultValue;
        }
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
        return splitsExecutor.submit(() -> getSplitsInternal(session, schemaTableName, maxSplitCount, nextToken));
    }

    private static PrestoThriftSplitBatch getSplitsInternal(
            PrestoThriftConnectorSession session,
            PrestoThriftSchemaTableName schemaTableName,
            int maxSplitCount,
            @Nullable byte[] nextToken)
    {
        int totalParts = getOrElse(session.getProperties(), NUMBER_OF_SPLITS_PARAMETER, DEFAULT_NUMBER_OF_SPLITS);
        // last sent part
        int partNumber = nextToken == null ? 0 : Ints.fromByteArray(nextToken);
        int numberOfSplits = min(maxSplitCount, totalParts - partNumber);

        List<PrestoThriftSplit> splits = new ArrayList<>(numberOfSplits);
        for (int i = 0; i < numberOfSplits; i++) {
            SplitInfo splitInfo = new SplitInfo(
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    partNumber + 1,
                    totalParts);
            byte[] splitId = serialize(splitInfo);
            splits.add(new PrestoThriftSplit(splitId, ImmutableList.of()));
            partNumber++;
        }
        byte[] newNextToken = partNumber < totalParts ? Ints.toByteArray(partNumber) : null;
        return new PrestoThriftSplitBatch(splits, newNextToken);
    }

    @Override
    public ListenableFuture<PrestoThriftRowsBatch> getRows(
            byte[] splitId,
            List<String> columns,
            long maxBytes,
            @Nullable byte[] nextToken)
    {
        return dataExecutor.submit(() -> getRowsInternal(splitId, columns, maxBytes, nextToken));
    }

    @Override
    public PrestoThriftNullableIndexLayoutResult resolveIndex(
            PrestoThriftConnectorSession session,
            PrestoThriftSchemaTableName schemaTableName,
            Set<String> indexableColumnNames,
            Set<String> outputColumnNames,
            PrestoThriftTupleDomain outputConstraint)
    {
        Optional<TpchIndexedData.IndexedTable> indexedTable = indexedData.getIndexedTable(
                schemaTableName.getTableName(),
                schemaNameToScaleFactor(schemaTableName.getSchemaName()),
                indexableColumnNames);
        if (!indexedTable.isPresent()) {
            return new PrestoThriftNullableIndexLayoutResult(null);
        }
        IndexInfo indexInfo = new IndexInfo(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                indexableColumnNames,
                ImmutableList.copyOf(outputColumnNames));
        return new PrestoThriftNullableIndexLayoutResult(new PrestoThriftIndexLayoutResult(serialize(indexInfo), outputConstraint));
    }

    @Override
    public ListenableFuture<PrestoThriftSplitsOrRows> getRowsOrSplitsForIndex(
            byte[] indexId,
            PrestoThriftRowsBatch keys,
            int maxSplitCount,
            long rowsMaxBytes,
            @Nullable byte[] nextToken)
    {
        return indexDataExecutor.submit(() -> getRowsForIndexInternal(indexId, keys, rowsMaxBytes, nextToken));
    }

    @PreDestroy
    @Override
    public void close()
    {
        splitsExecutor.shutdownNow();
        dataExecutor.shutdownNow();
        indexDataExecutor.shutdownNow();
    }

    private PrestoThriftSplitsOrRows getRowsForIndexInternal(
            byte[] indexId,
            PrestoThriftRowsBatch keys,
            long rowsMaxBytes,
            @Nullable byte[] nextToken)
    {
        IndexInfo indexInfo = deserialize(indexId, IndexInfo.class);
        TpchIndexedData.IndexedTable indexedTable = indexedData.getIndexedTable(
                indexInfo.getTableName(),
                schemaNameToScaleFactor(indexInfo.getSchemaName()),
                indexInfo.getIndexableColumnNames())
                .orElseThrow(() -> new IllegalArgumentException("Index is not present"));

        RecordSet keyRecordSet = batchToRecordSet(keys, indexInfo.getTableName(), indexedTable.getKeyColumns());
        RecordSet outputRecordSet = lookupIndexKeys(keyRecordSet, indexedTable, indexInfo.getOutputColumnNames());

        return new PrestoThriftSplitsOrRows(
                null,
                cursorToRowsBatch(
                        outputRecordSet.cursor(),
                        indexInfo.getOutputColumnNames(),
                        estimateRecords(rowsMaxBytes, outputRecordSet.getColumnTypes()),
                        nextToken));
    }

    /**
     * Convert a batch of index keys to record set which can be passed to index lookup.
     */
    private static RecordSet batchToRecordSet(PrestoThriftRowsBatch keys, String tableName, List<String> keyColumns)
    {
        List<Type> keyTypes = types(tableName, keyColumns);
        Page keysPage = convertToPage(keys, keyColumns, keyTypes);
        return new PageRecordSet(keyTypes, keysPage);
    }

    /**
     * Get lookup result and re-map output columns based on requested order.
     */
    private static RecordSet lookupIndexKeys(RecordSet keys, TpchIndexedData.IndexedTable table, List<String> outputColumnNames)
    {
        RecordSet allColumnsOutputRecordSet = table.lookupKeys(keys);
        List<Integer> outputRemap = computeRemap(table.getOutputColumns(), outputColumnNames);
        return new MappedRecordSet(allColumnsOutputRecordSet, outputRemap);
    }

    private static PrestoThriftRowsBatch getRowsInternal(byte[] splitId, List<String> columns, long maxBytes, @Nullable byte[] nextToken)
    {
        SplitInfo splitInfo = deserialize(splitId, SplitInfo.class);
        RecordCursor cursor = createCursor(splitInfo, columns);
        return cursorToRowsBatch(
                cursor,
                columns,
                estimateRecords(maxBytes, types(splitInfo.getTableName(), columns)),
                nextToken);
    }

    private static PrestoThriftRowsBatch cursorToRowsBatch(
            RecordCursor cursor,
            List<String> columnNames,
            int maxRowCount,
            @Nullable byte[] nextToken)
    {
        long skip = nextToken != null ? Longs.fromByteArray(nextToken) : 0;
        // very inefficient implementation as it needs to re-generate all previous results to get the next batch
        skipRows(cursor, skip);
        int numColumns = columnNames.size();

        // create and initialize writers
        List<ColumnWriter> writers = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            writers.add(ColumnWriters.create(columnNames.get(i), cursor.getType(i), min(maxRowCount, MAX_WRITERS_INITIAL_CAPACITY)));
        }

        // iterate over the cursor and append data to writers
        boolean hasNext = cursor.advanceNextPosition();
        int position;
        for (position = 0; position < maxRowCount && hasNext; position++) {
            for (int columnIdx = 0; columnIdx < numColumns; columnIdx++) {
                writers.get(columnIdx).append(cursor, columnIdx);
            }
            hasNext = cursor.advanceNextPosition();
        }

        // populate the final thrift result from writers
        List<PrestoThriftColumnData> result = new ArrayList<>(numColumns);
        for (ColumnWriter writer : writers) {
            result.addAll(writer.getResult());
        }

        return new PrestoThriftRowsBatch(result, position, hasNext ? Longs.toByteArray(skip + position) : null);
    }

    private static void skipRows(RecordCursor cursor, long numberOfRows)
    {
        boolean hasPreviousData = true;
        for (long i = 0; i < numberOfRows; i++) {
            hasPreviousData = cursor.advanceNextPosition();
        }
        checkState(hasPreviousData, "Cursor is expected to have previously generated data");
    }

    private static RecordCursor createCursor(SplitInfo splitInfo, List<String> columnNames)
    {
        switch (splitInfo.getTableName()) {
            case "orders":
                return createCursor(TpchTable.ORDERS, columnNames, splitInfo);
            case "customer":
                return createCursor(TpchTable.CUSTOMER, columnNames, splitInfo);
            case "lineitem":
                return createCursor(TpchTable.LINE_ITEM, columnNames, splitInfo);
            case "nation":
                return createCursor(TpchTable.NATION, columnNames, splitInfo);
            case "region":
                return createCursor(TpchTable.REGION, columnNames, splitInfo);
            case "part":
                return createCursor(TpchTable.PART, columnNames, splitInfo);
            default:
                throw new IllegalArgumentException("Table not setup: " + splitInfo.getTableName());
        }
    }

    private static <T extends TpchEntity> RecordCursor createCursor(TpchTable<T> table, List<String> columnNames, SplitInfo splitInfo)
    {
        List<TpchColumn<T>> columns = columnNames.stream().map(table::getColumn).collect(toList());
        return createTpchRecordSet(
                table,
                columns,
                schemaNameToScaleFactor(splitInfo.getSchemaName()),
                splitInfo.getPartNumber(),
                splitInfo.getTotalParts()).cursor();
    }
}
