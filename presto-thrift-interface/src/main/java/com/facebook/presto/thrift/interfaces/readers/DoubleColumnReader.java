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
import static java.lang.Double.doubleToLongBits;

final class DoubleColumnReader
{
    private DoubleColumnReader()
    {
    }

    private static void checkConsistency(boolean[] nulls, double[] doubles, int totalRecords)
    {
        checkArgument(totalRecords == 0 || nulls != null || doubles != null, "nulls array or values array must be present");
        checkArgument(nulls == null || nulls.length == totalRecords, "nulls must be null or of the expected size");
        checkArgument(doubles == null || doubles.length == totalRecords, "doubles must be null or of the expected size");
    }

    private static Block createBlock(boolean[] nulls, double[] doubles, int totalRecords)
    {
        long[] longs = new long[totalRecords];
        if (doubles != null) {
            for (int i = 0; i < totalRecords; i++) {
                longs[i] = doubleToLongBits(doubles[i]);
            }
        }
        return new LongArrayBlock(
                totalRecords,
                nulls == null ? new boolean[totalRecords] : nulls,
                longs);
    }

    public static Block readBlock(List<ThriftColumnData> columnsData, String columnName, int totalRecords)
    {
        ThriftColumnData columnData = ReaderUtils.columnByName(columnsData, columnName);
        checkArgument(columnData.isOnlyNullsAndDoubles(), "Remaining value containers must be null");
        boolean[] nulls = columnData.getNulls();
        double[] doubles = columnData.getDoubles();
        checkConsistency(nulls, doubles, totalRecords);
        return createBlock(nulls, doubles, totalRecords);
    }
}
