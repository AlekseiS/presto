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
package com.facebook.presto.connector.thrift.api.datatypes;

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnData;
import com.facebook.presto.connector.thrift.api.builders.AbstractIntColumnBuilder;
import com.facebook.presto.connector.thrift.api.builders.ColumnBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import static com.facebook.presto.connector.thrift.api.PrestoThriftColumnData.dateData;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkArgument;

@ThriftStruct
public final class PrestoThriftDate
        implements PrestoThriftColumnType
{
    private final boolean[] nulls;
    private final int[] dates;

    @ThriftConstructor
    public PrestoThriftDate(@Nullable boolean[] nulls, @Nullable int[] dates)
    {
        checkArgument(sameSizeIfPresent(nulls, dates), "nulls and values must be of the same size");
        this.nulls = nulls;
        this.dates = dates;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public boolean[] getNulls()
    {
        return nulls;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public int[] getDates()
    {
        return dates;
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        int numberOfRecords = numberOfRecords();
        return new IntArrayBlock(
                numberOfRecords,
                nulls == null ? new boolean[numberOfRecords] : nulls,
                dates == null ? new int[numberOfRecords] : dates);
    }

    @Override
    public int numberOfRecords()
    {
        return nulls != null ? nulls.length : (dates != null ? dates.length : 0);
    }

    public static ColumnBuilder builder(int initialCapacity)
    {
        return new AbstractIntColumnBuilder(initialCapacity)
        {
            @Override
            protected PrestoThriftColumnData buildInternal(boolean[] trimmedNulls, int[] trimmedInts)
            {
                return dateData(new PrestoThriftDate(trimmedNulls, trimmedInts));
            }
        };
    }

    private static boolean sameSizeIfPresent(boolean[] nulls, int[] dates)
    {
        return nulls == null || dates == null || nulls.length == dates.length;
    }
}
