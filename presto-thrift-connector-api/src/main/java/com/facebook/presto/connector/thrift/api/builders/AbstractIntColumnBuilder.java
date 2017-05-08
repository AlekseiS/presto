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
package com.facebook.presto.connector.thrift.api.builders;

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnData;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.util.Arrays;

import static com.facebook.presto.connector.thrift.api.builders.BuilderUtils.doubleCapacityChecked;
import static com.facebook.presto.connector.thrift.api.builders.BuilderUtils.trim;
import static com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractIntColumnBuilder
        implements ColumnBuilder
{
    private boolean[] nulls;
    private int[] ints;
    private int index;
    private boolean hasNulls;
    private boolean hasData;

    public AbstractIntColumnBuilder(int initialCapacity)
    {
        checkArgument(initialCapacity >= 0, "initialCapacity is negative");
        this.nulls = new boolean[initialCapacity];
        this.ints = new int[initialCapacity];
    }

    protected abstract PrestoThriftColumnData buildInternal(boolean[] trimmedNulls, int[] trimmedInts);

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

    private void appendNull()
    {
        if (index >= nulls.length) {
            nulls = Arrays.copyOf(nulls, doubleCapacityChecked(index));
        }
        nulls[index] = true;
        hasNulls = true;
        index++;
    }

    private void appendValue(int value)
    {
        if (index >= ints.length) {
            ints = Arrays.copyOf(ints, doubleCapacityChecked(index));
        }
        ints[index] = value;
        hasData = true;
        index++;
    }

    @Override
    public PrestoThriftColumnData build()
    {
        return buildInternal(trim(nulls, hasNulls, index), trim(ints, hasData, index));
    }
}
