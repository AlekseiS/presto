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
package com.facebook.presto.genericthrift.readers;

import com.facebook.presto.genericthrift.client.ThriftColumnData;
import com.facebook.presto.spi.type.Type;

import java.util.List;

import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.REAL;
import static com.facebook.presto.spi.type.StandardTypes.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.TIME;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TINYINT;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;

public final class ColumnReaders
{
    private ColumnReaders()
    {
    }

    public static ColumnReader createColumnReader(List<ThriftColumnData> columnsData, String columnName, Type columnType, int totalRecords)
    {
        switch (columnType.getTypeSignature().getBase()) {
            case BIGINT:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return LongColumnReader.createReader(columnsData, columnName, columnType, totalRecords);
            case DOUBLE:
            case REAL:
                return DoubleColumnReader.createReader(columnsData, columnName, columnType, totalRecords);
            case VARCHAR:
            case VARBINARY:
                return SliceColumnReader.createReader(columnsData, columnName, columnType, totalRecords);
            default:
                throw new IllegalArgumentException("Unsupported type: " + columnType);
        }
    }
}
