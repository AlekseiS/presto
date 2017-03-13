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

public class BooleanColumnWriter
        implements ColumnWriter
{
    private final String columnName;
    private boolean[] nulls;
    private boolean[] booleans;
    private int idx;
    private boolean hasNulls;
    private boolean hasData;

    public BooleanColumnWriter(String columnName, int initialCapacity)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        checkArgument(initialCapacity > 0, "initialCapacity is <=0");
        this.nulls = new boolean[initialCapacity];
        this.booleans = new boolean[initialCapacity];
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
            if (idx >= booleans.length) {
                booleans = Arrays.copyOf(booleans, 2 * idx);
            }
            booleans[idx] = cursor.getBoolean(field);
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
                null,
                trim(booleans, hasData, idx),
                null,
                null,
                columnName));
    }
}
