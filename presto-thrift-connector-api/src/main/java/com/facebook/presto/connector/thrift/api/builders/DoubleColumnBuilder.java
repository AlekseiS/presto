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
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftDouble;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.util.Arrays;

import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.doubleData;
import static com.facebook.presto.connector.thrift.api.builders.BuilderUtils.doubleCapacityChecked;
import static com.facebook.presto.connector.thrift.api.builders.BuilderUtils.trim;
import static com.google.common.base.Preconditions.checkArgument;

public class DoubleColumnBuilder
        implements ColumnBuilder
{
    private boolean[] nulls;
    private double[] doubles;
    private int index;
    private boolean hasNulls;
    private boolean hasData;

    public DoubleColumnBuilder(int initialCapacity)
    {
        checkArgument(initialCapacity >= 0, "initialCapacity is negative");
        this.nulls = new boolean[initialCapacity];
        this.doubles = new double[initialCapacity];
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
        if (index >= nulls.length) {
            nulls = Arrays.copyOf(nulls, doubleCapacityChecked(index));
        }
        nulls[index] = true;
        hasNulls = true;
        index++;
    }

    private void appendValue(double value)
    {
        if (index >= doubles.length) {
            doubles = Arrays.copyOf(doubles, doubleCapacityChecked(index));
        }
        doubles[index] = value;
        hasData = true;
        index++;
    }

    @Override
    public PrestoThriftBlock build()
    {
        return doubleData(new PrestoThriftDouble(trim(nulls, hasNulls, index), trim(doubles, hasData, index)));
    }
}
