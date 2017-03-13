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
package com.facebook.presto.genericthrift.writers;

import com.facebook.presto.genericthrift.client.ThriftColumnData;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.genericthrift.writers.WriterUtils.trim;
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
            if (idx >= nulls.length) {
                nulls = Arrays.copyOf(nulls, 2 * idx);
            }
            nulls[idx] = true;
            hasNulls = true;
        }
        else {
            if (idx >= doubles.length) {
                doubles = Arrays.copyOf(doubles, 2 * idx);
            }
            doubles[idx] = cursor.getDouble(field);
            hasData = true;
        }
        idx++;
    }

    @Override
    public List<ThriftColumnData> getResult()
    {
        return ImmutableList.of(new ThriftColumnData(
                trim(nulls, hasNulls, idx),
                null,
                trim(doubles, hasData, idx),
                null,
                null,
                null,
                columnName));
    }
}
