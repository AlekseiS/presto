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
package com.facebook.presto.thrift.interfaces.client;

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
    private final int[] ints;
    private final byte[] bytes;
    private final double[] doubles;
    private final String columnName;

    @ThriftConstructor
    public ThriftColumnData(
            @Nullable boolean[] nulls,
            @Nullable long[] longs,
            @Nullable int[] ints,
            @Nullable byte[] bytes,
            @Nullable double[] doubles,
            String columnName)
    {
        this.nulls = nulls;
        this.longs = longs;
        this.ints = ints;
        this.bytes = bytes;
        this.doubles = doubles;
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
    public int[] getInts()
    {
        return ints;
    }

    @ThriftField(value = 4, requiredness = OPTIONAL)
    public byte[] getBytes()
    {
        return bytes;
    }

    @ThriftField(value = 5, requiredness = OPTIONAL)
    public double[] getDoubles()
    {
        return doubles;
    }

    @ThriftField(value = 6)
    public String getColumnName()
    {
        return columnName;
    }

    public boolean isOnlyNullsAndLongs()
    {
        return ints == null && bytes == null && doubles == null;
    }

    public boolean isOnlyNullsAndDoubles()
    {
        return longs == null && ints == null && bytes == null;
    }

    public boolean isOnlyNullsIntsAndBytes()
    {
        return longs == null && doubles == null;
    }

    public boolean isOnlyNullsAndBytes()
    {
        return longs == null && ints == null && doubles == null;
    }

    public boolean isOnlyNullsAndInts()
    {
        return longs == null && bytes == null && doubles == null;
    }

    public long getDataSize()
    {
        return (nulls != null ? nulls.length : 0L) +
                (longs != null ? longs.length * Long.BYTES : 0L) +
                (ints != null ? ints.length * Integer.BYTES : 0L) +
                (bytes != null ? bytes.length : 0L) +
                (doubles != null ? doubles.length * Double.BYTES : 0L);
    }
}
