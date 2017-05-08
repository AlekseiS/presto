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
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableSchemaName;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftPage;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftServiceException;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.connector.thrift.api.builders.ColumnBuilder;
import com.facebook.presto.connector.thrift.server.states.SplitInfo;
import com.facebook.presto.spi.RecordCursor;
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
import java.util.Set;

import static com.facebook.presto.connector.thrift.server.TpchServerUtils.estimateRecords;
import static com.facebook.presto.connector.thrift.server.TpchServerUtils.getTypeString;
import static com.facebook.presto.connector.thrift.server.TpchServerUtils.schemaNameToScaleFactor;
import static com.facebook.presto.connector.thrift.server.TpchServerUtils.types;
import static com.facebook.presto.connector.thrift.server.states.SerializationUtils.deserialize;
import static com.facebook.presto.connector.thrift.server.states.SerializationUtils.serialize;
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
    private static final List<String> SCHEMAS = ImmutableList.of("tiny", "sf1");
    private static final int MAX_WRITERS_INITIAL_CAPACITY = 8192;

    private final ListeningExecutorService splitsExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("splits-generator-%s")));
    private final ListeningExecutorService dataExecutor =
            listeningDecorator(newCachedThreadPool(threadsNamed("data-generator-%s")));

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

    @Override
    public ListenableFuture<PrestoThriftSplitBatch> getSplits(
            PrestoThriftSchemaTableName schemaTableName,
            @Nullable Set<String> desiredColumns,
            PrestoThriftTupleDomain outputConstraint,
            int maxSplitCount,
            @Nullable byte[] nextToken)
            throws PrestoThriftServiceException
    {
        return splitsExecutor.submit(() -> getSplitsInternal(schemaTableName, maxSplitCount, nextToken));
    }

    private static PrestoThriftSplitBatch getSplitsInternal(
            PrestoThriftSchemaTableName schemaTableName,
            int maxSplitCount,
            @Nullable byte[] nextToken)
    {
        int totalParts = DEFAULT_NUMBER_OF_SPLITS;
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
    public ListenableFuture<PrestoThriftPage> getRows(
            byte[] splitId,
            List<String> columns,
            long maxBytes,
            @Nullable byte[] nextToken)
    {
        return dataExecutor.submit(() -> getRowsInternal(splitId, columns, maxBytes, nextToken));
    }

    @PreDestroy
    @Override
    public void close()
    {
        splitsExecutor.shutdownNow();
        dataExecutor.shutdownNow();
    }

    private static PrestoThriftPage getRowsInternal(byte[] splitId, List<String> columns, long maxBytes, @Nullable byte[] nextToken)
    {
        SplitInfo splitInfo = deserialize(splitId, SplitInfo.class);
        RecordCursor cursor = createCursor(splitInfo, columns);
        return cursorToRowsBatch(
                cursor,
                columns,
                estimateRecords(maxBytes, types(splitInfo.getTableName(), columns)),
                nextToken);
    }

    private static PrestoThriftPage cursorToRowsBatch(
            RecordCursor cursor,
            List<String> columnNames,
            int maxRowCount,
            @Nullable byte[] nextToken)
    {
        long skip = nextToken != null ? Longs.fromByteArray(nextToken) : 0;
        // very inefficient implementation as it needs to re-generate all previous results to get the next batch
        skipRows(cursor, skip);
        int numColumns = columnNames.size();

        // create and initialize builders
        List<ColumnBuilder> builders = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            builders.add(PrestoThriftColumnData.builder(cursor.getType(i), min(maxRowCount, MAX_WRITERS_INITIAL_CAPACITY)));
        }

        // iterate over the cursor and append data to builders
        boolean hasNext = cursor.advanceNextPosition();
        int position;
        for (position = 0; position < maxRowCount && hasNext; position++) {
            for (int columnIdx = 0; columnIdx < numColumns; columnIdx++) {
                builders.get(columnIdx).append(cursor, columnIdx);
            }
            hasNext = cursor.advanceNextPosition();
        }

        // populate the final thrift result from builders
        List<PrestoThriftColumnData> result = new ArrayList<>(numColumns);
        for (ColumnBuilder builder : builders) {
            result.add(builder.build());
        }

        return new PrestoThriftPage(result, position, hasNext ? Longs.toByteArray(skip + position) : null);
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
