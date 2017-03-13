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
package com.facebook.presto.genericthrift.client;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class ThriftColumnData
{
    private final boolean[] nulls;
    private final long[] longs;
    private final double[] doubles;
    private final boolean[] booleans;
    private final byte[] bytes;
    private final int[] sizes;
    private final String columnName;

    @ThriftConstructor
    public ThriftColumnData(
            @Nullable boolean[] nulls,
            @Nullable long[] longs,
            @Nullable double[] doubles,
            @Nullable boolean[] booleans,
            @Nullable byte[] bytes,
            @Nullable int[] sizes,
            String columnName)
    {
        this.nulls = nulls;
        this.longs = longs;
        this.doubles = doubles;
        this.booleans = booleans;
        this.bytes = bytes;
        this.sizes = sizes;
        this.columnName = requireNonNull(columnName, "columnName is null");
    }

    @ThriftField(value = 1, requiredness = OPTIONAL)
    public boolean[] getNulls()
    {
        return nulls;
    }

    @ThriftField(value = 2, requiredness = OPTIONAL)
    public long[] getLongs()
    {
        return longs;
    }

    @ThriftField(value = 3, requiredness = OPTIONAL)
    public double[] getDoubles()
    {
        return doubles;
    }

    @ThriftField(value = 4, requiredness = OPTIONAL)
    public boolean[] getBooleans()
    {
        return booleans;
    }

    @ThriftField(value = 5, requiredness = OPTIONAL)
    public byte[] getBytes()
    {
        return bytes;
    }

    @ThriftField(value = 6, requiredness = OPTIONAL)
    public int[] getSizes()
    {
        return sizes;
    }

    @ThriftField(value = 7)
    public String getColumnName()
    {
        return columnName;
    }

    public static ThriftColumnData empty(String columnName)
    {
        return new ThriftColumnData(
                null,
                null,
                null,
                null,
                null,
                null,
                columnName);
    }
}
