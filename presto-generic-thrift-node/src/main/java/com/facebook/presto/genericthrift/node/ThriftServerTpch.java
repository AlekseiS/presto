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
package com.facebook.presto.genericthrift.node;

import com.facebook.presto.genericthrift.client.ThriftColumnData;
import com.facebook.presto.genericthrift.client.ThriftColumnMetadata;
import com.facebook.presto.genericthrift.client.ThriftConnectorSession;
import com.facebook.presto.genericthrift.client.ThriftNullableTableMetadata;
import com.facebook.presto.genericthrift.client.ThriftPrestoClient;
import com.facebook.presto.genericthrift.client.ThriftPropertyMetadata;
import com.facebook.presto.genericthrift.client.ThriftRowsBatch;
import com.facebook.presto.genericthrift.client.ThriftSchemaTableName;
import com.facebook.presto.genericthrift.client.ThriftSessionValue;
import com.facebook.presto.genericthrift.client.ThriftSplit;
import com.facebook.presto.genericthrift.client.ThriftSplitBatch;
import com.facebook.presto.genericthrift.client.ThriftTableLayout;
import com.facebook.presto.genericthrift.client.ThriftTableLayoutResult;
import com.facebook.presto.genericthrift.client.ThriftTableMetadata;
import com.facebook.presto.genericthrift.client.ThriftTupleDomain;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.tpch.TpchMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.Slice;
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
import java.util.Set;

import static com.facebook.presto.tpch.TpchMetadata.ROW_NUMBER_COLUMN_NAME;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toList;

public class ThriftServerTpch
        implements ThriftPrestoClient
{
    private static final int DEFAULT_NUMBER_OF_SPLITS = 3;
    private static final String NUMBER_OF_SPLITS_PARAMETER = "splits";
    private static final List<String> SCHEMAS = ImmutableList.of("tiny", "sf1");
    private final ObjectMapper mapper = new ObjectMapper();
    private final ListeningExecutorService splitsExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("splits-generator-%s")));
    private final ListeningExecutorService dataExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("data-generator-%s")));

    @Override
    public List<ThriftPropertyMetadata> listSessionProperties()
    {
        return ImmutableList.of(new ThriftPropertyMetadata(NUMBER_OF_SPLITS_PARAMETER, "integer", "Number of splits",
                new ThriftSessionValue(false, null, 3, null, null, null),
                false));
    }

    @Override
    public List<String> listSchemaNames(ThriftConnectorSession session)
    {
        return SCHEMAS;
    }

    @Override
    public List<ThriftSchemaTableName> listTables(ThriftConnectorSession session, @Nullable String schemaNameOrNull)
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
    public ThriftNullableTableMetadata getTableMetadata(ThriftConnectorSession session, ThriftSchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        if (!SCHEMAS.contains(schemaName) || TpchTable.getTables().stream().noneMatch(table -> table.getTableName().equals(tableName))) {
            return new ThriftNullableTableMetadata(null);
        }
        TpchTable<?> tpchTable = TpchTable.getTable(schemaTableName.getTableName());
        List<ThriftColumnMetadata> columns = new ArrayList<>();
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new ThriftColumnMetadata(column.getColumnName(), getThriftType(column.getType()), null, false));
        }
        columns.add(new ThriftColumnMetadata(ROW_NUMBER_COLUMN_NAME, "bigint", null, true));
        return new ThriftNullableTableMetadata(new ThriftTableMetadata(schemaTableName, columns));
    }

    private static String getThriftType(TpchColumnType tpchType)
    {
        return TpchMetadata.getPrestoType(tpchType).getTypeSignature().toString();
    }

    @Override
    public List<ThriftTableLayoutResult> getTableLayouts(ThriftConnectorSession session, ThriftSchemaTableName schemaTableName, ThriftTupleDomain outputConstraint, @Nullable Set<String> desiredColumns)
    {
        return ImmutableList.of(new ThriftTableLayoutResult(
                // this layout is capable of returning all columns regardless of desired ones
                new ThriftTableLayout(null, ThriftTupleDomain.fromTupleDomain(TupleDomain.all())), outputConstraint));
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
    public ListenableFuture<ThriftSplitBatch> getSplitBatch(
            ThriftConnectorSession session,
            ThriftSchemaTableName schemaTableName,
            ThriftTableLayout layout,
            int maxSplitCount,
            @Nullable String continuationToken)
    {
        return splitsExecutor.submit(() -> getSplitBatchInternal(session, schemaTableName, maxSplitCount, continuationToken));
    }

    private ThriftSplitBatch getSplitBatchInternal(
            ThriftConnectorSession session,
            ThriftSchemaTableName schemaTableName,
            int maxSplitCount,
            @Nullable String continuationToken)
    {
        int totalParts = getOrElse(session.getProperties(), NUMBER_OF_SPLITS_PARAMETER, DEFAULT_NUMBER_OF_SPLITS);
        // last sent part
        int partNumber = continuationToken == null ? 0 : Integer.parseInt(continuationToken);
        int numberOfSplits = Math.min(maxSplitCount, totalParts - partNumber);

        List<ThriftSplit> splits = new ArrayList<>(numberOfSplits);
        for (int i = 0; i < numberOfSplits; i++) {
            SplitInfo splitInfo = new SplitInfo(
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    partNumber + 1,
                    totalParts);
            String splitId;
            try {
                splitId = mapper.writeValueAsString(splitInfo);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            splits.add(new ThriftSplit(splitId, ImmutableList.of()));
            partNumber++;
        }
        String nextToken = partNumber < totalParts ? String.valueOf(partNumber) : null;
        return new ThriftSplitBatch(splits, nextToken);
    }

    @Override
    public ListenableFuture<ThriftRowsBatch> getRows(String splitId, List<String> columnNames, int maxRowCount, @Nullable String continuationToken)
    {
        return dataExecutor.submit(() -> getRowsInternal(splitId, columnNames, maxRowCount, continuationToken));
    }

    private ThriftRowsBatch getRowsInternal(String splitId, List<String> columnNames, int maxRowCount, @Nullable String continuationToken)
    {
        requireNonNull(columnNames, "columnNames is null");
        SplitInfo splitInfo;
        try {
            splitInfo = mapper.readValue(splitId, SplitInfo.class);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        RecordCursor cursor = createCursor(splitInfo, columnNames);

        long skip = continuationToken != null ? Long.valueOf(continuationToken) : 0;
        // very inefficient implementation as it needs to re-generate all previous results to get the next batch
        for (long i = 0; i < skip; i++) {
            checkState(cursor.advanceNextPosition(), "Cursor is expected to have data");
        }
        int numColumns = columnNames.size();
        List<ThriftColumnData.Builder> columns = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            columns.add(new ThriftColumnData.Builder());
        }
        boolean hasNext = cursor.advanceNextPosition();
        int rowIdx;
        for (rowIdx = 0; rowIdx < maxRowCount && hasNext; rowIdx++) {
            for (int columnIdx = 0; columnIdx < numColumns; columnIdx++) {
                Class<?> javaType = cursor.getType(columnIdx).getJavaType();
                ThriftColumnData.Builder builder = columns.get(columnIdx);
                if (cursor.isNull(columnIdx)) {
                    builder.setNull(rowIdx);
                }
                else if (javaType == long.class) {
                    builder.setLong(rowIdx, cursor.getLong(columnIdx));
                }
                else if (javaType == double.class) {
                    builder.setDouble(rowIdx, cursor.getDouble(columnIdx));
                }
                else if (javaType == boolean.class) {
                    builder.setBoolean(rowIdx, cursor.getBoolean(columnIdx));
                }
                else if (javaType == Slice.class) {
                    builder.setSlice(rowIdx, cursor.getSlice(columnIdx));
                }
                else {
                    throw new IllegalArgumentException("Unsupported type: " + cursor.getType(columnIdx));
                }
            }
            hasNext = cursor.advanceNextPosition();
        }
        for (int columnIdx = 0; columnIdx < numColumns; columnIdx++) {
            columns.get(columnIdx).setColumnName(columnNames.get(columnIdx));
        }
        return new ThriftRowsBatch(
                columns.stream().map(ThriftColumnData.Builder::build).collect(toList()),
                rowIdx,
                hasNext ? String.valueOf(skip + rowIdx) : null);
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
        }
        throw new IllegalArgumentException("Table not setup: " + splitInfo.getTableName());
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

    @PreDestroy
    @Override
    public void close()
    {
        splitsExecutor.shutdownNow();
        dataExecutor.shutdownNow();
    }

    private static final class SplitInfo
    {
        private final String schemaName;
        private final String tableName;
        private final int partNumber;
        private final int totalParts;

        @JsonCreator
        public SplitInfo(
                @JsonProperty("schemaName") String schemaName,
                @JsonProperty("tableName") String tableName,
                @JsonProperty("partNumber") int partNumber,
                @JsonProperty("totalParts") int totalParts)
        {
            this.schemaName = requireNonNull(schemaName, "schemaName is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.partNumber = partNumber;
            this.totalParts = totalParts;
        }

        @JsonProperty
        public String getSchemaName()
        {
            return schemaName;
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }

        @JsonProperty
        public int getPartNumber()
        {
            return partNumber;
        }

        @JsonProperty
        public int getTotalParts()
        {
            return totalParts;
        }
    }
}
