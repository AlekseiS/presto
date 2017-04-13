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

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.thrift.interfaces.writers.WriterUtils.doubleCapacityChecked;
import static com.facebook.presto.thrift.interfaces.writers.WriterUtils.trim;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractByteColumnWriter
        implements ColumnWriter
{
    private final String columnName;
    private boolean[] nulls;
    private byte[] bytes;
    private int index;
    private boolean hasNulls;
    private boolean hasData;

    public AbstractByteColumnWriter(String columnName, int initialCapacity)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        checkArgument(initialCapacity > 0, "initialCapacity is negative or zero");
        this.nulls = new boolean[initialCapacity];
        this.bytes = new byte[initialCapacity];
    }

    public abstract byte readByte(RecordCursor cursor, int field);

    public abstract byte readByte(Block block, int position, Type type);

    @Override
    public final void append(RecordCursor cursor, int field)
    {
        if (cursor.isNull(field)) {
            appendNull();
        }
        else {
            appendValue(readByte(cursor, field));
        }
    }

    @Override
    public void append(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            appendNull();
        }
        else {
            appendValue(readByte(block, position, type));
        }
    }

    private void appendNull()
    {
        if (index >= nulls.length) {
            nulls = Arrays.copyOf(nulls, doubleCapacityChecked(index));
        }
        nulls[index] = true;
        hasNulls = true;
        index++;
    }

    private void appendValue(byte value)
    {
        if (index >= bytes.length) {
            bytes = Arrays.copyOf(bytes, doubleCapacityChecked(index));
        }
        bytes[index] = value;
        hasData = true;
        index++;
    }

    @Override
    public final List<ThriftColumnData> getResult()
    {
        return ImmutableList.of(new ThriftColumnData(
                trim(nulls, hasNulls, index),
                null,
                null,
                trim(bytes, hasData, index),
                null,
                columnName));
    }
}
