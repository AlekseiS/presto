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

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.util.Arrays;

import static com.facebook.presto.connector.thrift.api.builders.BuilderUtils.doubleCapacityChecked;
import static com.facebook.presto.connector.thrift.api.builders.BuilderUtils.trim;
import static com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractLongColumnBuilder
        implements ColumnBuilder
{
    private boolean[] nulls;
    private long[] longs;
    private int index;
    private boolean hasNulls;
    private boolean hasData;

    public AbstractLongColumnBuilder(int initialCapacity)
    {
        checkArgument(initialCapacity >= 0, "initialCapacity is negative");
        this.nulls = new boolean[initialCapacity];
        this.longs = new long[initialCapacity];
    }

    protected abstract PrestoThriftBlock buildInternal(boolean[] trimmedNulls, long[] trimmedLongs);

    @Override
    public final void append(RecordCursor cursor, int field)
    {
        if (cursor.isNull(field)) {
            appendNull();
        }
        else {
            appendValue(cursor.getLong(field));
        }
    }

    @Override
    public final void append(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            appendNull();
        }
        else {
            appendValue(type.getLong(block, position));
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

    private void appendValue(long value)
    {
        if (index >= longs.length) {
            longs = Arrays.copyOf(longs, doubleCapacityChecked(index));
        }
        longs[index] = value;
        hasData = true;
        index++;
    }

    @Override
    public final PrestoThriftBlock build()
    {
        return buildInternal(trim(nulls, hasNulls, index), trim(longs, hasData, index));
    }
}
