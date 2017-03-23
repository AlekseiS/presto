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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DoubleColumnWriter
        implements ColumnWriter
{
    private final String columnName;
    private boolean[] nulls;
    private double[] doubles;
    private int idx;
    private boolean hasNulls;
    private boolean hasData;

    public DoubleColumnWriter(String columnName, int initialCapacity)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        checkArgument(initialCapacity > 0, "initialCapacity is <=0");
        this.nulls = new boolean[initialCapacity];
        this.doubles = new double[initialCapacity];
    }

    @Override
    public void append(RecordCursor cursor, int field)
    {
        if (cursor.isNull(field)) {
            appendNull();
        }
        else {
            appendValue(cursor.getDouble(field));
        }
    }

    @Override
    public void append(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            appendNull();
        }
        else {
            appendValue(type.getDouble(block, position));
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

    private void appendValue(double value)
    {
        if (idx >= doubles.length) {
            doubles = Arrays.copyOf(doubles, 2 * idx);
        }
        doubles[idx] = value;
        hasData = true;
        idx++;
    }

    @Override
    public List<ThriftColumnData> getResult()
    {
        return ImmutableList.of(new ThriftColumnData(
                WriterUtils.trim(nulls, hasNulls, idx),
                null,
                null,
                null,
                WriterUtils.trim(doubles, hasData, idx),
                columnName));
    }
}
