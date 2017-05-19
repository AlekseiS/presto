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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

final class SliceType
        implements PrestoThriftColumnType
{
    private final boolean[] nulls;
    private final int[] sizes;
    private final byte[] bytes;

    public SliceType(@Nullable boolean[] nulls, @Nullable int[] sizes, @Nullable byte[] bytes)
    {
        checkArgument(sameSizeIfPresent(nulls, sizes), "nulls and values must be of the same size");
        checkArgument(totalSize(nulls, sizes) == (bytes != null ? bytes.length : 0), "total bytes size doesn't match expected size");
        this.nulls = nulls;
        this.sizes = sizes;
        this.bytes = bytes;
    }

    @Nullable
    public boolean[] getNulls()
    {
        return nulls;
    }

    @Nullable
    public int[] getSizes()
    {
        return sizes;
    }

    @Nullable
    public byte[] getBytes()
    {
        return bytes;
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        checkArgument(desiredType.getJavaType() == Slice.class, "type doesn't match: %s", desiredType);
        Slice values = bytes == null ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(bytes);
        int numberOfRecords = numberOfRecords();
        return new VariableWidthBlock(
                numberOfRecords,
                values,
                calculateOffsets(sizes, nulls, numberOfRecords),
                nulls == null ? new boolean[numberOfRecords] : nulls);
    }

    @Override
    public int numberOfRecords()
    {
        return nulls != null ? nulls.length : (sizes != null ? sizes.length : 0);
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
        SliceType other = (SliceType) obj;
        return Arrays.equals(this.nulls, other.nulls) &&
                Arrays.equals(this.sizes, other.sizes) &&
                Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(nulls), Arrays.hashCode(sizes), Arrays.hashCode(bytes));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRecords", numberOfRecords())
                .toString();
    }

    private static boolean sameSizeIfPresent(boolean[] nulls, int[] sizes)
    {
        return nulls == null || sizes == null || nulls.length == sizes.length;
    }

    private static int totalSize(boolean[] nulls, int[] sizes)
    {
        int numberOfRecords = nulls != null ? nulls.length : sizes != null ? sizes.length : 0;
        int total = 0;
        for (int i = 0; i < numberOfRecords; i++) {
            if (nulls == null || !nulls[i]) {
                total += sizes[i];
            }
        }
        return total;
    }

    private static int[] calculateOffsets(int[] sizes, boolean[] nulls, int totalRecords)
    {
        if (sizes == null) {
            return new int[totalRecords + 1];
        }
        int[] offsets = new int[totalRecords + 1];
        offsets[0] = 0;
        for (int i = 0; i < totalRecords; i++) {
            int size = nulls != null && nulls[i] ? 0 : sizes[i];
            offsets[i + 1] = offsets[i] + size;
        }
        return offsets;
    }
}
