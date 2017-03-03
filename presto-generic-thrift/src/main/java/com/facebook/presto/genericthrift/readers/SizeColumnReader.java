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
package com.facebook.presto.genericthrift.readers;

import com.facebook.presto.genericthrift.client.ThriftColumnData;
import com.facebook.presto.spi.type.Type;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SizeColumnReader
{
    private final boolean[] nulls;
    private final long[] longs;

    public SizeColumnReader(boolean[] nulls, long[] longs, int totalRecords)
    {
        checkArgument(totalRecords >= 0, "totalRecords must be non-negative");
        checkArgument(totalRecords == 0 || nulls != null || longs != null, "nulls array or values array must be present");
        checkArgument(nulls == null || nulls.length == totalRecords, "nulls must be null or of the expected size");
        checkArgument(longs == null || longs.length == totalRecords, "longs must be null or of the expected size");
        checkConsistency(nulls, longs, totalRecords);
        this.nulls = nulls;
        this.longs = longs;
    }

    private static void checkConsistency(boolean[] nulls, long[] longs, int totalRecords)
    {
        for (int i = 0; i < totalRecords; i++) {
            if (nulls != null && nulls[i] && longs[i] != 0) {
                throw new IllegalArgumentException("value in 'longs' array must be zero when the element is null");
            }
            // check the value is integer
            toIntExact(longs[i]);
        }
    }

    public int sum(int begin, int end)
    {
        long result = 0L;
        for (int i = begin; i < end; i++) {
            result += longs[i];
        }
        return toIntExact(result);
    }

    public int sum()
    {
        return sum(0, longs.length);
    }

    public int get(int idx)
    {
        return (int) longs[idx];
    }

    public static SizeColumnReader createReader(List<ThriftColumnData> columnsData, String columnName, Type type, int totalRecords)
    {
        requireNonNull(columnName, "columnName must be non-null");
        ThriftColumnData columnData = ReaderUtils.columnByName(columnsData, columnName);
        checkArgument(columnData.getBooleans() == null
                        && columnData.getDoubles() == null
                        && columnData.getBytes() == null
                        && columnData.getSizes() == null,
                "Remaining value containers must be null");
        return new SizeColumnReader(columnData.getNulls(), columnData.getLongs(), totalRecords);
    }

    public boolean[] nullValues(int begin, int end)
    {
        return (begin == 0 && end == nulls.length) ? nulls : Arrays.copyOfRange(nulls, begin, end);
    }
}
