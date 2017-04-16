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
package com.facebook.presto.thrift.node;

import com.facebook.presto.operator.index.PageRecordSet;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.split.MappedRecordSet;
import com.facebook.presto.tests.tpch.TpchIndexedData;
import com.facebook.presto.thrift.interfaces.client.ThriftColumnData;
import com.facebook.presto.thrift.interfaces.client.ThriftColumnMetadata;
import com.facebook.presto.thrift.interfaces.client.ThriftConnectorSession;
import com.facebook.presto.thrift.interfaces.client.ThriftIndexLayoutResult;
import com.facebook.presto.thrift.interfaces.client.ThriftNullableIndexLayoutResult;
import com.facebook.presto.thrift.interfaces.client.ThriftNullableTableMetadata;
import com.facebook.presto.thrift.interfaces.client.ThriftPrestoClient;
import com.facebook.presto.thrift.interfaces.client.ThriftPropertyMetadata;
import com.facebook.presto.thrift.interfaces.client.ThriftRowsBatch;
import com.facebook.presto.thrift.interfaces.client.ThriftSchemaTableName;
import com.facebook.presto.thrift.interfaces.client.ThriftSessionValue;
import com.facebook.presto.thrift.interfaces.client.ThriftSplit;
import com.facebook.presto.thrift.interfaces.client.ThriftSplitBatch;
import com.facebook.presto.thrift.interfaces.client.ThriftSplitsOrRows;
import com.facebook.presto.thrift.interfaces.client.ThriftTableLayout;
import com.facebook.presto.thrift.interfaces.client.ThriftTableLayoutResult;
import com.facebook.presto.thrift.interfaces.client.ThriftTableMetadata;
import com.facebook.presto.thrift.interfaces.client.ThriftTupleDomain;
import com.facebook.presto.thrift.interfaces.writers.ColumnWriter;
import com.facebook.presto.thrift.interfaces.writers.ColumnWriters;
import com.facebook.presto.thrift.node.states.IndexInfo;
import com.facebook.presto.thrift.node.states.LayoutInfo;
import com.facebook.presto.thrift.node.states.SplitInfo;
import com.facebook.presto.thrift.node.states.SplitsToken;
import com.facebook.presto.tpch.TpchMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.tpch.CustomerGenerator;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.OrderGenerator;
import io.airlift.tpch.PartGenerator;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.tests.AbstractTestIndexedQueries.INDEX_SPEC;
import static com.facebook.presto.thrift.interfaces.readers.ColumnReaders.convertToPage;
import static com.facebook.presto.tpch.TpchMetadata.ROW_NUMBER_COLUMN_NAME;
import static com.facebook.presto.tpch.TpchMetadata.getPrestoType;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toList;

public class ThriftTpchServer
        implements ThriftPrestoClient
{
    private static final int DEFAULT_NUMBER_OF_SPLITS = 3;
    private static final String NUMBER_OF_SPLITS_PARAMETER = "splits";
    private static final List<String> SCHEMAS = ImmutableList.of("tiny", "sf1");
    private static final int MAX_WRITERS_INITIAL_CAPACITY = 8192;
    private static final long BYTES_PER_PART = 2L * 1024L * 1024L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final ListeningExecutorService splitsExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("splits-generator-%s")));
    private final ListeningExecutorService dataExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("data-generator-%s")));
    private final ListeningExecutorService indexDataExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("index-data-generator-%s")));
    private final TpchIndexedData indexedData = new TpchIndexedData("tpchindexed", INDEX_SPEC);

    @Override
    public List<ThriftPropertyMetadata> listSessionProperties()
    {
        return ImmutableList.of(new ThriftPropertyMetadata(NUMBER_OF_SPLITS_PARAMETER, "integer", "Number of splits",
                new ThriftSessionValue(false, null, 3, null, null, null),
                false));
    }

    @Override
    public List<String> listSchemaNames()
    {
        return SCHEMAS;
    }

    @Override
    public List<ThriftSchemaTableName> listTables(@Nullable String schemaNameOrNull)
    {
        List<ThriftSchemaTableName> result = new ArrayList<>();
        for (String schemaName : getSchemaNames(schemaNameOrNull)) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                result.add(new ThriftSchemaTableName(schemaName, tpchTable.getTableName()));
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
    public ThriftNullableTableMetadata getTableMetadata(ThriftSchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        if (!SCHEMAS.contains(schemaName) || TpchTable.getTables().stream().noneMatch(table -> table.getTableName().equals(tableName))) {
            return new ThriftNullableTableMetadata(null);
        }
        TpchTable<?> tpchTable = TpchTable.getTable(schemaTableName.getTableName());
        List<ThriftColumnMetadata> columns = new ArrayList<>();
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new ThriftColumnMetadata(column.getSimplifiedColumnName(), getTypeString(column.getType()), null, false));
        }
        columns.add(new ThriftColumnMetadata(ROW_NUMBER_COLUMN_NAME, "bigint", null, true));
        return new ThriftNullableTableMetadata(new ThriftTableMetadata(schemaTableName, columns));
    }

    private static String getTypeString(TpchColumnType tpchType)
    {
        return TpchMetadata.getPrestoType(tpchType).getTypeSignature().toString();
    }

    @Override
    public List<ThriftTableLayoutResult> getTableLayouts(
            ThriftConnectorSession session,
            ThriftSchemaTableName schemaTableName,
            ThriftTupleDomain outputConstraint,
            @Nullable Set<String> desiredColumns)
    {
        List<String> columns = desiredColumns != null ? ImmutableList.copyOf(desiredColumns) : allColumns(schemaTableName.getTableName());
        byte[] layoutId = serialize(new LayoutInfo(schemaTableName.getSchemaName(), schemaTableName.getTableName(), columns));
        return ImmutableList.of(new ThriftTableLayoutResult(new ThriftTableLayout(layoutId, outputConstraint), outputConstraint));
    }

    private static int getOrElse(Map<String, ThriftSessionValue> values, String name, int defaultValue)
    {
        ThriftSessionValue parameterValue = values.get(name);
        if (parameterValue != null && !parameterValue.isNullValue()) {
            return parameterValue.getIntValue();
        }
        else {
            return defaultValue;
        }
    }

    @Override
    public ListenableFuture<ThriftSplitBatch> getSplits(
            ThriftConnectorSession session,
            ThriftTableLayout layout,
            int maxSplitCount,
            @Nullable byte[] nextToken)
    {
        return splitsExecutor.submit(() -> getSplitsInternal(session, layout, maxSplitCount, nextToken));
    }

    private static ThriftSplitBatch getSplitsInternal(
            ThriftConnectorSession session,
            ThriftTableLayout layout,
            int maxSplitCount,
            @Nullable byte[] nextToken)
    {
        LayoutInfo layoutInfo = deserialize(layout.getLayoutId(), LayoutInfo.class);
        int nextPart;
        final int partsPerSplit;
        final int totalParts;
        if (nextToken == null) {
            int totalSplits = getOrElse(session.getProperties(), NUMBER_OF_SPLITS_PARAMETER, DEFAULT_NUMBER_OF_SPLITS);
            int recordsPerPart = estimateRecords(BYTES_PER_PART, types(layoutInfo.getTableName(), layoutInfo.getColumnNames()));
            int totalRecords = totalRecords(layoutInfo.getSchemaName(), layoutInfo.getTableName());
            partsPerSplit = totalRecords / totalSplits / recordsPerPart + 1;
            nextPart = 1;
            totalParts = totalSplits * partsPerSplit;
        }
        else {
            SplitsToken token = deserialize(nextToken, SplitsToken.class);
            partsPerSplit = token.getPartsPerSplit();
            nextPart = token.getNextPart();
            totalParts = token.getTotalParts();
        }

        int remainingSplits = (totalParts - nextPart + 1) / partsPerSplit;
        int numberOfSplits = min(maxSplitCount, remainingSplits);
        List<ThriftSplit> splits = new ArrayList<>(numberOfSplits);
        for (int i = 0; i < numberOfSplits; i++) {
            SplitInfo splitInfo = new SplitInfo(
                    layoutInfo.getSchemaName(),
                    layoutInfo.getTableName(),
                    nextPart,
                    nextPart + partsPerSplit - 1,
                    totalParts,
                    layoutInfo.getColumnNames());
            byte[] splitId = serialize(splitInfo);
            splits.add(new ThriftSplit(splitId, ImmutableList.of()));
            nextPart += partsPerSplit;
            remainingSplits--;
        }

        byte[] newNextToken = remainingSplits > 0 ? serialize(new SplitsToken(nextPart, partsPerSplit, totalParts)) : null;
        return new ThriftSplitBatch(splits, newNextToken);
    }

    @Override
    public ListenableFuture<ThriftRowsBatch> getRows(byte[] splitId, long maxBytes, @Nullable byte[] nextToken)
    {
        return dataExecutor.submit(() -> getRowsInternal(splitId, maxBytes, nextToken));
    }

    @Override
    public ThriftNullableIndexLayoutResult resolveIndex(
            ThriftConnectorSession session,
            ThriftSchemaTableName schemaTableName,
            Set<String> indexableColumnNames,
            Set<String> outputColumnNames,
            ThriftTupleDomain outputConstraint)
    {
        Optional<TpchIndexedData.IndexedTable> indexedTable = indexedData.getIndexedTable(
                schemaTableName.getTableName(),
                schemaNameToScaleFactor(schemaTableName.getSchemaName()),
                indexableColumnNames);
        if (!indexedTable.isPresent()) {
            return new ThriftNullableIndexLayoutResult(null);
        }
        IndexInfo indexInfo = new IndexInfo(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                indexableColumnNames,
                ImmutableList.copyOf(outputColumnNames));
        return new ThriftNullableIndexLayoutResult(new ThriftIndexLayoutResult(serialize(indexInfo), outputConstraint));
    }

    @Override
    public ListenableFuture<ThriftSplitsOrRows> getRowsOrSplitsForIndex(
            byte[] indexId,
            ThriftRowsBatch keys,
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

    private ThriftSplitsOrRows getRowsForIndexInternal(
            byte[] indexId,
            ThriftRowsBatch keys,
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

        return new ThriftSplitsOrRows(
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
    private static RecordSet batchToRecordSet(ThriftRowsBatch keys, String tableName, List<String> keyColumns)
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

    private static ThriftRowsBatch getRowsInternal(byte[] splitId, long maxBytes, @Nullable byte[] nextToken)
    {
        checkArgument(maxBytes >= BYTES_PER_PART,
                "Requested max bytes is less than bytes per single part: %s < %s", maxBytes, BYTES_PER_PART);

        SplitInfo splitInfo = deserialize(splitId, SplitInfo.class);
        List<String> columnNames = splitInfo.getColumnNames();
        List<Type> columnTypes = types(splitInfo.getTableName(), columnNames);
        int numColumns = columnNames.size();

        // create and initialize writers
        List<ColumnWriter> writers = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            writers.add(ColumnWriters.create(columnNames.get(i), columnTypes.get(i)));
        }

        // write a necessary number of parts
        int nextPartToWrite = nextToken != null ? Ints.fromByteArray(nextToken) : splitInfo.getStartPartNumber();
        int endPart = min(splitInfo.getEndPartNumber(), nextPartToWrite + toIntExact(maxBytes / BYTES_PER_PART) - 1);
        int recordsWritten = 0;
        for (int part = nextPartToWrite; part <= endPart; part++) {
            RecordCursor cursor = createCursor(
                    splitInfo.getTableName(),
                    splitInfo.getSchemaName(),
                    columnNames,
                    part,
                    splitInfo.getTotalParts());
            while (cursor.advanceNextPosition()) {
                for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
                    writers.get(columnIndex).append(cursor, columnIndex);
                }
                recordsWritten++;
            }
        }

        // populate the final thrift result from writers
        List<ThriftColumnData> result = new ArrayList<>(numColumns);
        for (ColumnWriter writer : writers) {
            result.addAll(writer.getResult());
        }

        return new ThriftRowsBatch(result, recordsWritten,
                endPart < splitInfo.getEndPartNumber() ? Ints.toByteArray(endPart + 1) : null);
    }

    private static int estimateRecords(long maxBytes, List<Type> types)
    {
        if (types.isEmpty()) {
            // no data will be returned, only row count
            return Integer.MAX_VALUE;
        }
        int bytesPerRow = 0;
        for (Type type : types) {
            if (type instanceof FixedWidthType) {
                bytesPerRow += ((FixedWidthType) type).getFixedSize();
            }
            else if (type instanceof VarcharType) {
                VarcharType varchar = (VarcharType) type;
                if (varchar.isUnbounded()) {
                    // random estimate
                    bytesPerRow += 32;
                }
                else {
                    bytesPerRow += varchar.getLengthSafe();
                }
            }
            else {
                throw new IllegalArgumentException("Cannot compute estimates for an unknown type: " + type);
            }
        }
        return toIntExact(maxBytes / bytesPerRow);
    }

    private static ThriftRowsBatch cursorToRowsBatch(
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
        List<ThriftColumnData> result = new ArrayList<>(numColumns);
        for (ColumnWriter writer : writers) {
            result.addAll(writer.getResult());
        }

        return new ThriftRowsBatch(result, position, hasNext ? Longs.toByteArray(skip + position) : null);
    }

    private static void skipRows(RecordCursor cursor, long numberOfRows)
    {
        boolean hasPreviousData = true;
        for (long i = 0; i < numberOfRows; i++) {
            hasPreviousData = cursor.advanceNextPosition();
        }
        checkState(hasPreviousData, "Cursor is expected to have previously generated data");
    }

    private static List<Integer> computeRemap(List<String> startSchema, List<String> endSchema)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (String columnName : endSchema) {
            int index = startSchema.indexOf(columnName);
            checkArgument(index != -1, "Column name in end that is not in the start: %s", columnName);
            builder.add(index);
        }
        return builder.build();
    }

    private static List<String> allColumns(String tableName)
    {
        return TpchTable.getTable(tableName).getColumns().stream().map(TpchColumn::getSimplifiedColumnName).collect(toList());
    }

    private static List<Type> types(String tableName, List<String> columnNames)
    {
        TpchTable<?> table = TpchTable.getTable(tableName);
        return columnNames.stream().map(name -> getPrestoType(table.getColumn(name).getType())).collect(toList());
    }

    private static <T extends TpchEntity> RecordCursor createCursor(
            String tableName,
            String schemaName,
            List<String> columnNames,
            int partNumber,
            int totalParts)
    {
        switch (tableName) {
            case "orders":
                return createCursor(TpchTable.ORDERS, schemaName, columnNames, partNumber, totalParts);
            case "customer":
                return createCursor(TpchTable.CUSTOMER, schemaName, columnNames, partNumber, totalParts);
            case "lineitem":
                return createCursor(TpchTable.LINE_ITEM, schemaName, columnNames, partNumber, totalParts);
            case "nation":
                return createCursor(TpchTable.NATION, schemaName, columnNames, partNumber, totalParts);
            case "region":
                return createCursor(TpchTable.REGION, schemaName, columnNames, partNumber, totalParts);
            case "part":
                return createCursor(TpchTable.PART, schemaName, columnNames, partNumber, totalParts);
            default:
                throw new IllegalArgumentException("Table not setup: " + tableName);
        }
    }

    private static <T extends TpchEntity> RecordCursor createCursor(
            TpchTable<T> table,
            String schemaName,
            List<String> columnNames,
            int partNumber,
            int totalParts)
    {
        List<TpchColumn<T>> columns = columnNames.stream().map(table::getColumn).collect(toList());
        return createTpchRecordSet(
                table,
                columns,
                schemaNameToScaleFactor(schemaName),
                partNumber,
                totalParts).cursor();
    }

    private static int totalRecords(String schemaName, String tableName)
    {
        double scaleFactor = schemaNameToScaleFactor(schemaName);
        switch (tableName) {
            case "orders":
            case "lineitem":
                return doubleToIntChecked(scaleFactor * OrderGenerator.SCALE_BASE);
            case "customer":
                return doubleToIntChecked(scaleFactor * CustomerGenerator.SCALE_BASE);
            case "nation":
                return Distributions.getDefaultDistributions().getNations().size();
            case "region":
                return Distributions.getDefaultDistributions().getRegions().size();
            case "part":
                return doubleToIntChecked(scaleFactor * PartGenerator.SCALE_BASE);
            default:
                throw new IllegalArgumentException("Table not setup for total records: " + tableName);
        }
    }

    private static int doubleToIntChecked(double value)
    {
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new IllegalArgumentException("Double value is not an integer: " + value);
        }
        return (int) value;
    }

    private static double schemaNameToScaleFactor(String schemaName)
    {
        switch (schemaName) {
            case "tiny":
                return 0.01;
            case "sf1":
                return 1.0;
        }
        throw new IllegalArgumentException("Invalid schema name: " + schemaName);
    }

    private static byte[] serialize(Object value)
    {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(value);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> T deserialize(byte[] value, Class<T> tClass)
    {
        try {
            return OBJECT_MAPPER.readValue(value, tClass);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
