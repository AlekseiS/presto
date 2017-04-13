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
package com.facebook.presto.thrift.interfaces.readers;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.thrift.interfaces.client.ThriftColumnData;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

final class IntColumnReader
{
    private IntColumnReader()
    {
    }

    private static void checkConsistency(boolean[] nulls, int[] ints, int totalRecords)
    {
        checkArgument(totalRecords == 0 || nulls != null || ints != null, "nulls array or values array must be present");
        checkArgument(nulls == null || nulls.length == totalRecords, "nulls array must be null or of the expected size");
        checkArgument(ints == null || ints.length == totalRecords, "ints must be null or of the expected size");
    }

    public static Block readBlock(List<ThriftColumnData> columnsData, String columnName, int totalRecords)
    {
        ThriftColumnData columnData = ReaderUtils.columnByName(columnsData, columnName);
        checkArgument(columnData.isOnlyNullsAndInts(), "Remaining value containers must be null");
        boolean[] nulls = columnData.getNulls();
        int[] ints = columnData.getInts();
        checkConsistency(nulls, ints, totalRecords);
        return new IntArrayBlock(
                totalRecords,
                nulls == null ? new boolean[totalRecords] : nulls,
                ints == null ? new int[totalRecords] : ints);
    }

    public static NullsAndOffsets readNullsAndOffsets(List<ThriftColumnData> columnsData, String columnName, int totalRecords)
    {
        ThriftColumnData columnData = ReaderUtils.columnByName(columnsData, columnName);
        checkArgument(columnData.isOnlyNullsAndInts(), "Remaining value containers must be null");
        boolean[] nulls = columnData.getNulls();
        int[] ints = columnData.getInts();
        checkConsistency(nulls, ints, totalRecords);
        return new NullsAndOffsets(
                nulls == null ? new boolean[totalRecords] : nulls,
                ReaderUtils.calculateOffsets(ints, nulls, totalRecords));
    }

    public static final class NullsAndOffsets
    {
        private final boolean[] nulls;
        private final int[] offsets;

        public NullsAndOffsets(boolean[] nulls, int[] offsets)
        {
            this.nulls = requireNonNull(nulls, "nulls is null");
            this.offsets = requireNonNull(offsets, "offsets is null");
        }

        public boolean[] getNulls()
        {
            return nulls;
        }

        public int[] getOffsets()
        {
            return offsets;
        }
    }
}
