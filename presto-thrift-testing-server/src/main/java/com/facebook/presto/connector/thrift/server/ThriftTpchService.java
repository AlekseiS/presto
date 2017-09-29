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

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.connector.thrift.api.PrestoThriftColumnMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftId;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableColumnSet;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableSchemaName;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableToken;
import com.facebook.presto.connector.thrift.api.PrestoThriftPageResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftServiceException;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.MappedRecordSet;
import com.facebook.presto.tests.tpch.TpchIndexedData;
import com.facebook.presto.tests.tpch.TpchIndexedData.IndexedTable;
import com.facebook.presto.tests.tpch.TpchScaledTable;
import com.facebook.presto.tpch.TpchMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.fromBlock;
import static com.facebook.presto.connector.thrift.server.SplitInfo.indexSplit;
import static com.facebook.presto.connector.thrift.server.SplitInfo.normalSplit;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.tests.AbstractTestIndexedQueries.INDEX_SPEC;
import static com.facebook.presto.tpch.TpchMetadata.getPrestoType;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

public class ThriftTpchService
        implements PrestoThriftService
{
    private static final int DEFAULT_NUMBER_OF_SPLITS = 3;
    private static final int NUMBER_OF_INDEX_SPLITS = 2;
    private static final List<String> SCHEMAS = ImmutableList.of("tiny", "sf1");
    private static final JsonCodec<SplitInfo> SPLIT_INFO_CODEC = jsonCodec(SplitInfo.class);

    private final TpchIndexedData indexedData = new TpchIndexedData("tpchindexed", INDEX_SPEC);

    @Override
    public List<String> listSchemaNames()
    {
        return SCHEMAS;
    }

    @Override
    public List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
    {
        List<PrestoThriftSchemaTableName> tables = new ArrayList<>();
        for (String schemaName : getSchemaNames(schemaNameOrNull.getSchemaName())) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                tables.add(new PrestoThriftSchemaTableName(schemaName, tpchTable.getTableName()));
            }
        }
        return tables;
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
        TpchScaledTable tpchScaledTable = new TpchScaledTable(tableName, schemaNameToScaleFactor(schemaName));
        Set<Set<String>> indexableKeys = ImmutableSet.copyOf(INDEX_SPEC.getColumnIndexes(tpchScaledTable));
        return new PrestoThriftNullableTableMetadata(new PrestoThriftTableMetadata(schemaTableName, columns, null, !indexableKeys.isEmpty() ? indexableKeys : null));
    }

    @Override
    public ListenableFuture<PrestoThriftSplitBatch> getSplits(
            PrestoThriftSchemaTableName schemaTableName,
            PrestoThriftNullableColumnSet desiredColumns,
            PrestoThriftTupleDomain outputConstraint,
            int maxSplitCount,
            PrestoThriftNullableToken nextToken)
            throws PrestoThriftServiceException
    {
        int totalParts = DEFAULT_NUMBER_OF_SPLITS;
        // last sent part
        int partNumber = nextToken.getToken() == null ? 0 : Ints.fromByteArray(nextToken.getToken().getId());
        int numberOfSplits = min(maxSplitCount, totalParts - partNumber);

        List<PrestoThriftSplit> splits = new ArrayList<>(numberOfSplits);
        for (int i = 0; i < numberOfSplits; i++) {
            SplitInfo splitInfo = normalSplit(
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    partNumber + 1,
                    totalParts);
            splits.add(new PrestoThriftSplit(new PrestoThriftId(SPLIT_INFO_CODEC.toJsonBytes(splitInfo)), ImmutableList.of()));
            partNumber++;
        }
        PrestoThriftId newNextToken = partNumber < totalParts ? new PrestoThriftId(Ints.toByteArray(partNumber)) : null;
        return immediateFuture(new PrestoThriftSplitBatch(splits, newNextToken));
    }

    @Override
    public ListenableFuture<PrestoThriftSplitBatch> getLookupSplits(
            PrestoThriftSchemaTableName schemaTableName,
            List<String> lookupColumnNames,
            List<String> outputColumnNames,
            PrestoThriftPageResult keys,
            PrestoThriftTupleDomain outputConstraint,
            int maxSplitCount,
            PrestoThriftNullableToken nextToken)
            throws PrestoThriftServiceException
    {
        checkArgument(NUMBER_OF_INDEX_SPLITS <= maxSplitCount, "maxSplitCount for lookup splits is too low");
        checkArgument(nextToken.getToken() == null, "no continuation is supported for lookup splits");
        int totalKeys = keys.getRowCount();
        int partSize = totalKeys / NUMBER_OF_INDEX_SPLITS;
        List<PrestoThriftSplit> splits = new ArrayList<>(NUMBER_OF_INDEX_SPLITS);
        for (int splitIndex = 0; splitIndex < NUMBER_OF_INDEX_SPLITS; splitIndex++) {
            int begin = partSize * splitIndex;
            int end = partSize * (splitIndex + 1);
            if (splitIndex + 1 == NUMBER_OF_INDEX_SPLITS) {
                // add remainder to the last split
                end = totalKeys;
            }
            if (begin == end) {
                // split is empty, skip it
                continue;
            }
            SplitInfo splitInfo = indexSplit(
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    lookupColumnNames,
                    thriftPageToLongList(keys, begin, end));
            splits.add(new PrestoThriftSplit(new PrestoThriftId(SPLIT_INFO_CODEC.toJsonBytes(splitInfo)), ImmutableList.of()));
        }
        return immediateFuture(new PrestoThriftSplitBatch(splits, null));
    }

    @Override
    public ListenableFuture<PrestoThriftPageResult> getRows(
            PrestoThriftId splitId,
            List<String> outputColumns,
            long maxBytes,
            PrestoThriftNullableToken nextToken)
    {
        SplitInfo splitInfo = SPLIT_INFO_CODEC.fromJson(splitId.getId());
        checkArgument(maxBytes >= DEFAULT_MAX_PAGE_SIZE_IN_BYTES, "requested maxBytes is too small");
        ConnectorPageSource pageSource;
        if (!splitInfo.isIndexSplit()) {
            // normal scan
            pageSource = createPageSource(splitInfo, outputColumns);
        }
        else {
            // index lookup
            pageSource = createLookupPageSource(splitInfo, outputColumns);
        }
        return immediateFuture(getRowsInternal(pageSource, splitInfo.getTableName(), outputColumns, nextToken.getToken()));
    }

    @Override
    public void close()
    {
    }

    private static PrestoThriftPageResult getRowsInternal(ConnectorPageSource pageSource, String tableName, List<String> columnNames, @Nullable PrestoThriftId nextToken)
    {
        // very inefficient implementation as it needs to re-generate all previous results to get the next page
        int skipPages = nextToken != null ? Ints.fromByteArray(nextToken.getId()) : 0;
        skipPages(pageSource, skipPages);

        Page page = null;
        while (!pageSource.isFinished() && page == null) {
            page = pageSource.getNextPage();
            skipPages++;
        }
        PrestoThriftId newNextToken = pageSource.isFinished() ? null : new PrestoThriftId(Ints.toByteArray(skipPages));

        return toThriftPage(page, types(tableName, columnNames), newNextToken);
    }

    private ConnectorPageSource createLookupPageSource(SplitInfo splitInfo, List<String> outputColumnNames)
    {
        IndexedTable indexedTable = indexedData.getIndexedTable(
                splitInfo.getTableName(),
                schemaNameToScaleFactor(splitInfo.getSchemaName()),
                ImmutableSet.copyOf(splitInfo.getLookupColumnNames()))
                .orElseThrow(() -> new IllegalArgumentException("Index is not present"));
        List<Type> lookupColumnTypes = types(splitInfo.getTableName(), splitInfo.getLookupColumnNames());
        RecordSet keyRecordSet = new ListBasedRecordSet(splitInfo.getKeys(), lookupColumnTypes);
        RecordSet outputRecordSet = lookupIndexKeys(keyRecordSet, indexedTable, outputColumnNames);
        return new RecordPageSource(outputRecordSet);
    }

    /**
     * Get lookup result and re-map output columns based on requested order.
     */
    private static RecordSet lookupIndexKeys(RecordSet keys, IndexedTable table, List<String> outputColumnNames)
    {
        RecordSet allColumnsOutputRecordSet = table.lookupKeys(keys);
        List<Integer> outputRemap = computeRemap(table.getOutputColumns(), outputColumnNames);
        return new MappedRecordSet(allColumnsOutputRecordSet, outputRemap);
    }

    private static PrestoThriftPageResult toThriftPage(Page page, List<Type> columnTypes, @Nullable PrestoThriftId nextToken)
    {
        if (page == null) {
            checkState(nextToken == null, "there must be no more data when page is null");
            return new PrestoThriftPageResult(ImmutableList.of(), 0, null);
        }
        checkState(page.getChannelCount() == columnTypes.size(), "number of columns in a page doesn't match the one in requested types");
        int numberOfColumns = columnTypes.size();
        List<PrestoThriftBlock> columnBlocks = new ArrayList<>(numberOfColumns);
        for (int i = 0; i < numberOfColumns; i++) {
            columnBlocks.add(fromBlock(page.getBlock(i), columnTypes.get(i)));
        }
        return new PrestoThriftPageResult(columnBlocks, page.getPositionCount(), nextToken);
    }

    private static void skipPages(ConnectorPageSource pageSource, int skipPages)
    {
        for (int i = 0; i < skipPages; i++) {
            checkState(!pageSource.isFinished(), "pageSource is unexpectedly finished");
            pageSource.getNextPage();
        }
    }

    private static ConnectorPageSource createPageSource(SplitInfo splitInfo, List<String> columnNames)
    {
        switch (splitInfo.getTableName()) {
            case "orders":
                return createPageSource(TpchTable.ORDERS, columnNames, splitInfo);
            case "customer":
                return createPageSource(TpchTable.CUSTOMER, columnNames, splitInfo);
            case "lineitem":
                return createPageSource(TpchTable.LINE_ITEM, columnNames, splitInfo);
            case "nation":
                return createPageSource(TpchTable.NATION, columnNames, splitInfo);
            case "region":
                return createPageSource(TpchTable.REGION, columnNames, splitInfo);
            case "part":
                return createPageSource(TpchTable.PART, columnNames, splitInfo);
            default:
                throw new IllegalArgumentException("Table not setup: " + splitInfo.getTableName());
        }
    }

    private static <T extends TpchEntity> ConnectorPageSource createPageSource(TpchTable<T> table, List<String> columnNames, SplitInfo splitInfo)
    {
        List<TpchColumn<T>> columns = columnNames.stream().map(table::getColumn).collect(toList());
        return new RecordPageSource(createTpchRecordSet(
                table,
                columns,
                schemaNameToScaleFactor(splitInfo.getSchemaName()),
                splitInfo.getPartNumber(),
                splitInfo.getTotalParts(),
                Optional.empty()));
    }

    private static List<List<Long>> thriftPageToLongList(PrestoThriftPageResult page, int begin, int end)
    {
        checkArgument(begin <= end, "invalid interval");
        if (begin == end) {
            // empty interval
            return ImmutableList.of();
        }
        List<PrestoThriftBlock> blocks = page.getColumnBlocks();
        List<List<Long>> result = new ArrayList<>(blocks.size());
        for (PrestoThriftBlock block : blocks) {
            checkArgument(block.getBigintData() != null || block.getIntegerData() != null, "only bigint and integer are supported");
            result.add(numericBlockAsList(block, begin, end));
        }
        return result;
    }

    private static List<Long> numericBlockAsList(PrestoThriftBlock block, int begin, int end)
    {
        List<Long> result = new ArrayList<>(end - begin);
        if (block.getBigintData() != null) {
            boolean[] nulls = block.getBigintData().getNulls();
            long[] longs = block.getBigintData().getLongs();
            for (int index = begin; index < end; index++) {
                if (nulls != null && nulls[index]) {
                    result.add(null);
                }
                else {
                    checkArgument(longs != null, "block structure is incorrect");
                    result.add(longs[index]);
                }
            }
        }
        else if (block.getIntegerData() != null) {
            boolean[] nulls = block.getIntegerData().getNulls();
            int[] ints = block.getIntegerData().getInts();
            for (int index = begin; index < end; index++) {
                if (nulls != null && nulls[index]) {
                    result.add(null);
                }
                else {
                    checkArgument(ints != null, "block structure is incorrect");
                    result.add((long) ints[index]);
                }
            }
        }
        else {
            throw new IllegalArgumentException("Only bigint and integer blocks are supported");
        }
        return result;
    }

    private static List<Type> types(String tableName, List<String> columnNames)
    {
        TpchTable<?> table = TpchTable.getTable(tableName);
        return columnNames.stream().map(name -> getPrestoType(table.getColumn(name).getType())).collect(toList());
    }

    private static double schemaNameToScaleFactor(String schemaName)
    {
        switch (schemaName) {
            case "tiny":
                return 0.01;
            case "sf1":
                return 1.0;
        }
        throw new IllegalArgumentException("Schema is not setup: " + schemaName);
    }

    private static String getTypeString(TpchColumnType tpchType)
    {
        return TpchMetadata.getPrestoType(tpchType).getTypeSignature().toString();
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
}
