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
package com.facebook.presto.thrift.interfaces.readers;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.thrift.interfaces.client.ThriftColumnData;
import com.facebook.presto.thrift.interfaces.client.ThriftRowsBatch;

import javax.annotation.Nullable;

import java.util.List;

import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.CHAR;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.spi.type.StandardTypes.P4_HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.TIME;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TINYINT;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

public final class ColumnReaders
{
    private ColumnReaders()
    {
    }

    @Nullable
    public static Page convertToPage(ThriftRowsBatch rowsBatch, List<String> columnNames, List<Type> columnTypes)
    {
        checkArgument(columnNames.size() == columnTypes.size(), "columns and types have different sizes");
        if (rowsBatch.getRowCount() == 0) {
            return null;
        }
        int numberOfColumns = columnNames.size();
        if (numberOfColumns == 0) {
            // request/response with no columns, used for queries like select count star
            return new Page(rowsBatch.getRowCount());
        }
        List<ThriftColumnData> columnsData = rowsBatch.getColumnsData();
        Block[] blocks = new Block[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {
            blocks[i] = readBlock(columnsData, columnNames.get(i), columnTypes.get(i), rowsBatch.getRowCount());
        }
        return new Page(blocks);
    }

    public static Block readBlock(List<ThriftColumnData> columnsData, String columnName, Type columnType, int totalRecords)
    {
        checkArgument(totalRecords >= 0, "totalRecords is negative");
        switch (columnType.getTypeSignature().getBase()) {
            case BIGINT:
            case TIME:
            case TIMESTAMP:
                return LongColumnReader.readBlock(columnsData, columnName, totalRecords);
            case INTEGER:
            case DATE:
                return IntColumnReader.readBlock(columnsData, columnName, totalRecords);
            case TINYINT:
            case BOOLEAN:
                return ByteColumnReader.readBlock(columnsData, columnName, totalRecords);
            case DOUBLE:
                return DoubleColumnReader.readBlock(columnsData, columnName, totalRecords);
            case VARCHAR:
            case VARBINARY:
            case CHAR:
            case HYPER_LOG_LOG:
            case P4_HYPER_LOG_LOG:
            case JSON:
                return SliceColumnReader.readBlock(columnsData, columnName, totalRecords);
            case ARRAY:
                return ArrayColumnReader.readBlock(columnsData, columnName, columnType, totalRecords);
            default:
                throw new IllegalArgumentException("Unsupported type: " + columnType);
        }
    }
}
