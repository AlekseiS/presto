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
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.thrift.interfaces.client.ThriftColumnData;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

final class LongColumnReader
{
    private LongColumnReader()
    {
    }

    private static void checkConsistency(boolean[] nulls, long[] longs, int totalRecords)
    {
        checkArgument(totalRecords == 0 || nulls != null || longs != null, "nulls array or values array must be present");
        checkArgument(nulls == null || nulls.length == totalRecords, "nulls array must be null or of the expected size");
        checkArgument(longs == null || longs.length == totalRecords, "longs must be null or of the expected size");
    }

    public static Block readBlock(List<ThriftColumnData> columnsData, String columnName, int totalRecords)
    {
        ThriftColumnData columnData = ReaderUtils.columnByName(columnsData, columnName);
        checkArgument(columnData.isOnlyNullsAndLongs(), "Remaining value containers must be null");
        boolean[] nulls = columnData.getNulls();
        long[] longs = columnData.getLongs();
        checkConsistency(nulls, longs, totalRecords);
        return new LongArrayBlock(
                totalRecords,
                nulls == null ? new boolean[totalRecords] : nulls,
                longs == null ? new long[totalRecords] : longs);
    }
}
