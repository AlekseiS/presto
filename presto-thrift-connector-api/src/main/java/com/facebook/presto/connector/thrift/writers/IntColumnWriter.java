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
package com.facebook.presto.connector.thrift.writers;

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnData;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IntColumnWriter
        implements ColumnWriter
{
    private final String columnName;
    private boolean[] nulls;
    private int[] ints;
    private int index;
    private boolean hasNulls;
    private boolean hasData;

    public IntColumnWriter(String columnName, int initialCapacity)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        checkArgument(initialCapacity > 0, "initialCapacity is negative or zero");
        this.nulls = new boolean[initialCapacity];
        this.ints = new int[initialCapacity];
    }

    @Override
    public void append(RecordCursor cursor, int field)
    {
        if (cursor.isNull(field)) {
            appendNull();
        }
        else {
            appendValue((int) cursor.getLong(field));
        }
    }

    @Override
    public void append(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            appendNull();
        }
        else {
            appendValue((int) type.getLong(block, position));
        }
    }

    void appendNull()
    {
        if (index >= nulls.length) {
            nulls = Arrays.copyOf(nulls, WriterUtils.doubleCapacityChecked(index));
        }
        nulls[index] = true;
        hasNulls = true;
        index++;
    }

    void appendValue(int value)
    {
        if (index >= ints.length) {
            ints = Arrays.copyOf(ints, WriterUtils.doubleCapacityChecked(index));
        }
        ints[index] = value;
        hasData = true;
        index++;
    }

    @Override
    public List<PrestoThriftColumnData> getResult()
    {
        return ImmutableList.of(new PrestoThriftColumnData(
                WriterUtils.trim(nulls, hasNulls, index),
                null,
                WriterUtils.trim(ints, hasData, index),
                null,
                null,
                columnName));
    }
}
