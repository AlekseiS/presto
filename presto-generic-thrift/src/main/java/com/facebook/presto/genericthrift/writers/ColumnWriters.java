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
package com.facebook.presto.genericthrift.writers;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

public final class ColumnWriters
{
    public static final int INITIAL_CAPACITY = 1024;

    private ColumnWriters()
    {
    }

    public static ColumnWriter create(String columnName, Type columnType)
    {
        Class<?> javaType = columnType.getJavaType();
        if (javaType == long.class) {
            return new LongColumnWriter(columnName, INITIAL_CAPACITY);
        }
        else if (javaType == double.class) {
            return new DoubleColumnWriter(columnName, INITIAL_CAPACITY);
        }
        else if (javaType == boolean.class) {
            return new BooleanColumnWriter(columnName, INITIAL_CAPACITY);
        }
        else if (javaType == Slice.class) {
            return new BytesColumnWriter(columnName, INITIAL_CAPACITY);
        }
        else if (javaType == Block.class) {
            throw new UnsupportedOperationException("Complex types like array and map are not yet supported");
        }
        else {
            throw new IllegalArgumentException("Unsupported writer type: " + columnType);
        }
    }
}
