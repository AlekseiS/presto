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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.thrift.interfaces.client.ThriftColumnData;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SliceColumnWriter
        implements ColumnWriter
{
    private final String columnName;
    private boolean[] nulls;
    private byte[] bytes;
    private int[] sizes;
    private int idx;
    private int bytesIdx;
    private boolean hasNulls;
    private boolean hasData;

    public SliceColumnWriter(String columnName, int initialCapacity)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        checkArgument(initialCapacity > 0, "initialCapacity is <=0");
        this.nulls = new boolean[initialCapacity];
        this.bytes = new byte[initialCapacity];
        this.sizes = new int[initialCapacity];
    }

    @Override
    public void append(RecordCursor cursor, int field)
    {
        if (cursor.isNull(field)) {
            appendNull();
        }
        else {
            appendSlice(cursor.getSlice(field));
        }
    }

    @Override
    public void append(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            appendNull();
        }
        else {
            appendSlice(type.getSlice(block, position));
        }
    }

    private void appendNull()
    {
        if (idx >= nulls.length) {
            nulls = Arrays.copyOf(nulls, 2 * idx);
        }
        nulls[idx] = true;
        hasNulls = true;
        idx++;
    }

    private void appendSlice(Slice slice)
    {
        appendSize(slice.length());
        appendBytes(slice);
        hasData = true;
    }

    private void appendSize(int value)
    {
        if (idx >= sizes.length) {
            sizes = Arrays.copyOf(sizes, 2 * idx);
        }
        sizes[idx] = value;
        idx++;
    }

    private void appendBytes(Slice slice)
    {
        int length = slice.length();
        int newBytesLength = bytesIdx + length;
        if (newBytesLength >= bytes.length) {
            bytes = Arrays.copyOf(bytes, newBytesLength * 2);
        }
        slice.getBytes(0, bytes, bytesIdx, length);
        bytesIdx += length;
    }

    @Override
    public List<ThriftColumnData> getResult()
    {
        return ImmutableList.of(new ThriftColumnData(
                WriterUtils.trim(nulls, hasNulls, idx),
                null,
                WriterUtils.trim(sizes, hasData, idx),
                WriterUtils.trim(bytes, hasData, bytesIdx),
                null,
                columnName));
    }
}
