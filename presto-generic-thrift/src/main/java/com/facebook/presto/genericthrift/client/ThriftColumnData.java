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

import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkState;
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

    public static class Builder
    {
        private List<Boolean> nulls;
        private List<Long> longs;
        private List<Double> doubles;
        private List<Boolean> booleans;
        private List<Slice> slices;
        private String columnName;

        public Builder setNull(int idx)
        {
            if (nulls == null) {
                nulls = new ArrayList<>();
            }
            ensureSize(nulls, idx + 1, false);
            nulls.set(idx, true);
            return this;
        }

        public Builder setValue(int idx, Object value, Type type)
        {
            Class<?> javaType = type.getJavaType();
            if (value == null) {
                return setNull(idx);
            }
            else if (javaType == long.class) {
                return setLong(idx, (long) value);
            }
            else if (javaType == double.class) {
                return setDouble(idx, (double) value);
            }
            else if (javaType == boolean.class) {
                return setBoolean(idx, (boolean) value);
            }
            else if (javaType == Slice.class) {
                Slice slice;
                if (value instanceof byte[]) {
                    slice = Slices.wrappedBuffer((byte[]) value);
                }
                else if (value instanceof String) {
                    slice = Slices.utf8Slice((String) value);
                }
                else {
                    slice = (Slice) value;
                }
                return setSlice(idx, slice);
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }

        public Builder setLong(int idx, long value)
        {
            if (longs == null) {
                longs = new ArrayList<>();
            }
            ensureSize(longs, idx + 1, 0L);
            longs.set(idx, value);
            return this;
        }

        public Builder setDouble(int idx, double value)
        {
            if (doubles == null) {
                doubles = new ArrayList<>();
            }
            ensureSize(doubles, idx + 1, 0.0);
            doubles.set(idx, value);
            return this;
        }

        public Builder setBoolean(int idx, boolean value)
        {
            if (booleans == null) {
                booleans = new ArrayList<>();
            }
            ensureSize(booleans, idx + 1, false);
            booleans.set(idx, value);
            return this;
        }

        public Builder setSlice(int idx, Slice value)
        {
            if (slices == null) {
                slices = new ArrayList<>();
            }
            ensureSize(slices, idx + 1, null);
            slices.set(idx, value);
            return this;
        }

        public Builder setColumnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public ThriftColumnData build()
        {
            byte[] bytes = null;
            int[] sizes = null;
            if (slices != null && !slices.isEmpty()) {
                int totalSize = 0;
                for (Slice slice : slices) {
                    if (slice != null) {
                        totalSize += slice.length();
                    }
                }
                bytes = new byte[totalSize];
                sizes = new int[slices.size()];
                int nextOffset = 0;
                for (int idx = 0; idx < slices.size(); idx++) {
                    Slice slice = slices.get(idx);
                    if (slice != null) {
                        int sliceSize = slice.length();
                        sizes[idx] = sliceSize;
                        slice.getBytes(0, bytes, nextOffset, sliceSize);
                        nextOffset += sliceSize;
                    }
                    else {
                        sizes[idx] = 0;
                    }
                }
                checkState(nextOffset == totalSize, "Size of byte array doesn't match expected one: %s vs %s", nextOffset, totalSize);
            }
            return new ThriftColumnData(
                    nulls == null ? null : Booleans.toArray(nulls),
                    longs == null ? null : Longs.toArray(longs),
                    doubles == null ? null : Doubles.toArray(doubles),
                    booleans == null ? null : Booleans.toArray(booleans),
                    bytes,
                    sizes,
                    columnName);
        }

        private static <T> void ensureSize(List<T> list, int newSize, T value)
        {
            int size = list.size();
            int required = newSize - size;
            if (required <= 0) {
                return;
            }
            list.addAll(list.size(), Collections.nCopies(required, value));
        }
    }
}
