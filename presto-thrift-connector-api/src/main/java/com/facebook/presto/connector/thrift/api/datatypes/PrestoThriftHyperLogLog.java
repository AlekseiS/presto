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
import com.facebook.presto.connector.thrift.api.builders.AbstractSliceColumnBuilder;
import com.facebook.presto.connector.thrift.api.builders.ColumnBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.facebook.presto.connector.thrift.api.PrestoThriftColumnData.hyperLogLogData;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Each elements of {@code sizes} array contains the length in bytes for the corresponding element.
 * {@code bytes} array contains encoded byte values for HyperLogLog representation as defined in airlift specification.
 * Values for all rows are written to {@code bytes} array one after another.
 * The total number of bytes must be equal to the sum of all sizes.
 */
@ThriftStruct
public final class PrestoThriftHyperLogLog
        implements PrestoThriftColumnType
{
    private final SliceType sliceType;

    @ThriftConstructor
    public PrestoThriftHyperLogLog(@Nullable boolean[] nulls, @Nullable int[] sizes, @Nullable byte[] bytes)
    {
        this.sliceType = new SliceType(nulls, sizes, bytes);
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public boolean[] getNulls()
    {
        return sliceType.getNulls();
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public int[] getSizes()
    {
        return sliceType.getSizes();
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public byte[] getBytes()
    {
        return sliceType.getBytes();
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        checkArgument(HYPER_LOG_LOG.equals(desiredType), "type doesn't match: %s", desiredType);
        return sliceType.toBlock(desiredType);
    }

    @Override
    public int numberOfRecords()
    {
        return sliceType.numberOfRecords();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftHyperLogLog other = (PrestoThriftHyperLogLog) obj;
        return Objects.equals(this.sliceType, other.sliceType);
    }

    @Override
    public int hashCode()
    {
        return sliceType.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRecords", numberOfRecords())
                .toString();
    }

    public static ColumnBuilder builder(int initialCapacity)
    {
        return new AbstractSliceColumnBuilder(initialCapacity)
        {
            @Override
            protected PrestoThriftColumnData buildInternal(boolean[] trimmedNulls, int[] trimmedSizes, byte[] trimmedBytes)
            {
                return hyperLogLogData(new PrestoThriftHyperLogLog(trimmedNulls, trimmedSizes, trimmedBytes));
            }
        };
    }
}
