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

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.bigintArrayData;
import static com.facebook.presto.connector.thrift.api.datatypes.TypeUtils.calculateOffsets;
import static com.facebook.presto.connector.thrift.api.datatypes.TypeUtils.sameSizeIfPresent;
import static com.facebook.presto.connector.thrift.api.datatypes.TypeUtils.totalSize;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * TODO: write
 */
@ThriftStruct
public final class PrestoThriftBigintArray
        implements PrestoThriftColumnData
{
    private final boolean[] nulls;
    private final int[] sizes;
    private final PrestoThriftBigint values;

    @ThriftConstructor
    public PrestoThriftBigintArray(@Nullable boolean[] nulls, @Nullable int[] sizes, @Nullable PrestoThriftBigint values)
    {
        checkArgument(sameSizeIfPresent(nulls, sizes), "nulls and values must be of the same size");
        checkArgument(totalSize(nulls, sizes) == numberOfValues(values), "total number of values doesn't match expected size");
        this.nulls = nulls;
        this.sizes = sizes;
        this.values = values;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public boolean[] getNulls()
    {
        return nulls;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public int[] getSizes()
    {
        return sizes;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public PrestoThriftBigint getValues()
    {
        return values;
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        checkArgument(desiredType.getTypeParameters().size() == 1 && BIGINT.equals(desiredType.getTypeParameters().get(0)),
                "type doesn't match: %s", desiredType);
        int numberOfRecords = numberOfRecords();
        return new ArrayBlock(
                numberOfRecords,
                nulls == null ? new boolean[numberOfRecords] : nulls,
                calculateOffsets(sizes, nulls, numberOfRecords),
                values != null ? values.toBlock(BIGINT) : new LongArrayBlock(0, new boolean[] {}, new long[] {}));
    }

    @Override
    public int numberOfRecords()
    {
        return nulls != null ? nulls.length : (sizes != null ? sizes.length : 0);
    }

    private static int numberOfValues(PrestoThriftBigint values)
    {
        return values != null ? values.numberOfRecords() : 0;
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
        PrestoThriftBigintArray other = (PrestoThriftBigintArray) obj;
        return Arrays.equals(this.nulls, other.nulls) &&
                Arrays.equals(this.sizes, other.sizes) &&
                Objects.equals(this.values, other.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(nulls), Arrays.hashCode(sizes), values);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRecords", numberOfRecords())
                .toString();
    }

    public static PrestoThriftBlock fromSingleValueBlock(Block block, Type type)
    {
        if (block.isNull(0)) {
            return bigintArrayData(new PrestoThriftBigintArray(new boolean[] {true}, null, null));
        }
        else {
            Block value = (Block) type.getObject(block, 0);
            PrestoThriftBigint bigintData = PrestoThriftBigint.fromBlock(value);
            return bigintArrayData(new PrestoThriftBigintArray(
                    null,
                    new int[] {bigintData.numberOfRecords()},
                    bigintData));
        }
    }
}
