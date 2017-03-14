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

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;

import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class ThriftRowsBatch
{
    private final List<ThriftColumnData> columnsData;
    private final int rowCount;
    private final byte[] nextToken;

    @ThriftConstructor
    public ThriftRowsBatch(List<ThriftColumnData> columnsData, int rowCount, byte[] nextToken)
    {
        this.columnsData = requireNonNull(columnsData, "columnsData is null");
        checkArgument(rowCount >= 0, "rowCount is negative");
        this.rowCount = rowCount;
        this.nextToken = nextToken;
    }

    @ThriftField(1)
    public List<ThriftColumnData> getColumnsData()
    {
        return columnsData;
    }

    @ThriftField(2)
    public int getRowCount()
    {
        return rowCount;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public byte[] getNextToken()
    {
        return nextToken;
    }

    public static ThriftRowsBatch empty()
    {
        return new ThriftRowsBatch(ImmutableList.of(), 0, null);
    }
}
