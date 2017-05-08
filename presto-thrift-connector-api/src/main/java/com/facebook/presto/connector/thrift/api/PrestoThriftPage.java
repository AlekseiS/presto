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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.connector.thrift.api.utils.ByteUtils.summarize;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftPage
{
    private final List<PrestoThriftColumnData> columnsData;
    private final Integer rowCount;
    private final byte[] nextToken;

    @ThriftConstructor
    public PrestoThriftPage(List<PrestoThriftColumnData> columnsData, @Nullable Integer rowCount, @Nullable byte[] nextToken)
    {
        this.columnsData = requireNonNull(columnsData, "columnsData is null");
        checkConsistency(columnsData, rowCount);
        this.rowCount = rowCount;
        this.nextToken = nextToken;
    }

    /**
     * Returns data in a columnar format.
     * Columns in this list must be in the order they were requested by the engine.
     */
    @ThriftField(1)
    public List<PrestoThriftColumnData> getColumnsData()
    {
        return columnsData;
    }

    /**
     * Number of rows is optional when at least one column is present.
     * However, it is required when a list of columns is empty, such as when sending data for "count star" queries.
     */
    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public Integer getRowCount()
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
        int numberOfRows = numberOfRows();
        if (numberOfRows == 0) {
            return null;
        }
        int numberOfColumns = columnsData.size();
        if (numberOfColumns == 0) {
            // request/response with no columns, used for queries like "select count star"
            return new Page(numberOfRows);
        }
        Block[] blocks = new Block[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {
            blocks[i] = columnsData.get(i).toBlock(columnTypes.get(i));
        }
        return new Page(blocks);
    }

    private int numberOfRows()
    {
        if (rowCount != null) {
            return rowCount;
        }
        return columnsData.get(0).numberOfRecords();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnsData, rowCount, Arrays.hashCode(nextToken));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftPage other = (PrestoThriftPage) obj;
        return Objects.equals(this.columnsData, other.columnsData) &&
                Objects.equals(this.rowCount, other.rowCount) &&
                Arrays.equals(this.nextToken, other.nextToken);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnsData", columnsData)
                .add("rowCount", rowCount)
                .add("nextToken", summarize(nextToken))
                .toString();
    }

    private static void checkConsistency(List<PrestoThriftColumnData> columnsData, Integer rowCount)
    {
        if (rowCount != null) {
            checkArgument(rowCount >= 0, "rowCount must be non-negative when present");
            checkAllColumnsAreOfExpectedSize(columnsData, rowCount);
        }
        else {
            checkArgument(!columnsData.isEmpty(), "rowCount must be present when list of columns is empty");
            checkAllColumnsAreOfExpectedSize(columnsData, columnsData.get(0).numberOfRecords());
        }
    }

    private static void checkAllColumnsAreOfExpectedSize(List<PrestoThriftColumnData> columnsData, int expectedNumberOfRows)
    {
        for (int i = 0; i < columnsData.size(); i++) {
            checkArgument(columnsData.get(i).numberOfRecords() == expectedNumberOfRows,
                    "Incorrect number of records for column with index %s: expected %s, got %s",
                    i, expectedNumberOfRows, columnsData.get(i).numberOfRecords());
        }
    }
}
