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
package com.facebook.presto.connector.thrift.api;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import java.util.List;

import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftPage
{
    private final List<PrestoThriftColumnData> columnsData;
    private final int rowCount;
    private final byte[] nextToken;

    @ThriftConstructor
    public PrestoThriftPage(List<PrestoThriftColumnData> columnsData, int rowCount, @Nullable byte[] nextToken)
    {
        this.columnsData = requireNonNull(columnsData, "columnsData is null");
        checkArgument(rowCount >= 0, "rowCount is negative");
        checkAllColumnsAreOfExpectedSize(columnsData, rowCount);
        this.rowCount = rowCount;
        this.nextToken = nextToken;
    }

    @ThriftField(1)
    public List<PrestoThriftColumnData> getColumnsData()
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

    @Nullable
    public Page toPage(List<Type> columnTypes)
    {
        checkArgument(columnsData.size() == columnTypes.size(), "columns and types have different sizes");
        if (rowCount == 0) {
            return null;
        }
        int numberOfColumns = columnsData.size();
        if (numberOfColumns == 0) {
            // request/response with no columns, used for queries like select count star
            return new Page(rowCount);
        }
        Block[] blocks = new Block[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {
            blocks[i] = columnsData.get(i).toBlock(columnTypes.get(i));
        }
        return new Page(blocks);
    }

    private static void checkAllColumnsAreOfExpectedSize(List<PrestoThriftColumnData> columnsData, int rowCount)
    {
        for (int i = 0; i < columnsData.size(); i++) {
            checkArgument(columnsData.get(i).numberOfRecords() == rowCount,
                    "Incorrect number of records for column with index %s: expected %s, got %s", i, rowCount, columnsData.get(i).numberOfRecords());
        }
    }
}
