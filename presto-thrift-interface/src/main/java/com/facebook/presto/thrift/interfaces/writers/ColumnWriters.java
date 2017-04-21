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
package com.facebook.presto.thrift.interfaces.writers;

import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.CHAR;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.P4_HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.TIME;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TINYINT;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.google.common.collect.Iterables.getOnlyElement;

public final class ColumnWriters
{
    public static final int DEFAULT_INITIAL_CAPACITY = 1024;

    private ColumnWriters()
    {
    }

    public static ColumnWriter create(String columnName, Type columnType, int initialCapacity)
    {
        switch (columnType.getTypeSignature().getBase()) {
            case BIGINT:
            case TIME:
            case TIMESTAMP:
                return new LongColumnWriter(columnName, initialCapacity);
            case INTEGER:
            case DATE:
                return new IntColumnWriter(columnName, initialCapacity);
            case TINYINT:
                return new TinyintColumnWriter(columnName, initialCapacity);
            case BOOLEAN:
                return new BooleanColumnWriter(columnName, initialCapacity);
            case DOUBLE:
                return new DoubleColumnWriter(columnName, initialCapacity);
            case VARCHAR:
            case VARBINARY:
            case CHAR:
            case HYPER_LOG_LOG:
            case P4_HYPER_LOG_LOG:
                return new SliceColumnWriter(columnName, initialCapacity);
            case ARRAY:
                return new ArrayColumnWriter(columnName, initialCapacity, getOnlyElement(columnType.getTypeParameters()));
            default:
                throw new IllegalArgumentException("Unsupported writer type: " + columnType);
        }
    }

    public static ColumnWriter create(String columnName, Type columnType)
    {
        return create(columnName, columnType, DEFAULT_INITIAL_CAPACITY);
    }
}
