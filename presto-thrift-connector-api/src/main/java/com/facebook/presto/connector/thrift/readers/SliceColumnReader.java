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
package com.facebook.presto.connector.thrift.readers;

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnData;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.VariableWidthBlock;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.connector.thrift.readers.ReaderUtils.calculateOffsets;
import static com.facebook.presto.connector.thrift.readers.ReaderUtils.getColumnDataByName;
import static com.google.common.base.Preconditions.checkArgument;

public final class SliceColumnReader
{
    private SliceColumnReader()
    {
    }

    private static void checkConsistency(boolean[] nulls, byte[] bytes, int[] sizes, int totalRecords)
    {
        checkArgument(totalRecords == 0 || nulls != null || bytes != null, "nulls array or values array must be present");
        checkArgument(nulls == null || nulls.length == totalRecords, "nulls array must be null or of the expected size");
        checkArgument(sizes == null || sizes.length == totalRecords, "sizes must be null or of the expected size");
        checkArgument(sizes == null || bytes != null, "bytes must be present when sizes is present");
    }

    private static Block createBlock(boolean[] nulls, byte[] bytes, int[] sizes, int totalRecords)
    {
        Slice values = bytes == null ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(bytes);
        return new VariableWidthBlock(
                totalRecords,
                values,
                calculateOffsets(sizes, nulls, totalRecords),
                nulls == null ? new boolean[totalRecords] : nulls);
    }

    public static Block readBlock(List<PrestoThriftColumnData> columnsData, String columnName, int totalRecords)
    {
        PrestoThriftColumnData columnData = getColumnDataByName(columnsData, columnName);
        checkArgument(columnData.isOnlyNullsIntsAndBytes(), "Remaining value containers must be null");
        boolean[] nulls = columnData.getNulls();
        byte[] bytes = columnData.getBytes();
        int[] sizes = columnData.getInts();
        checkConsistency(nulls, bytes, sizes, totalRecords);
        return createBlock(nulls, bytes, sizes, totalRecords);
    }
}
