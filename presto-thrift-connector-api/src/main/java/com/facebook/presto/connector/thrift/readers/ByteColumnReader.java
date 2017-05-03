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
import com.facebook.presto.spi.block.ByteArrayBlock;

import java.util.List;

import static com.facebook.presto.connector.thrift.readers.ReaderUtils.getColumnDataByName;
import static com.google.common.base.Preconditions.checkArgument;

public final class ByteColumnReader
{
    private ByteColumnReader()
    {
    }

    private static void checkConsistency(boolean[] nulls, byte[] bytes, int totalRecords)
    {
        checkArgument(totalRecords == 0 || nulls != null || bytes != null, "nulls array or values array must be present");
        checkArgument(nulls == null || nulls.length == totalRecords, "nulls array must be null or of the expected size");
        checkArgument(bytes == null || bytes.length == totalRecords, "bytes must be null or of the expected size");
    }

    public static Block readBlock(List<PrestoThriftColumnData> columnsData, String columnName, int totalRecords)
    {
        PrestoThriftColumnData columnData = getColumnDataByName(columnsData, columnName);
        checkArgument(columnData.isOnlyNullsAndBytes(), "Remaining value containers must be null");
        boolean[] nulls = columnData.getNulls();
        byte[] bytes = columnData.getBytes();
        checkConsistency(nulls, bytes, totalRecords);
        return new ByteArrayBlock(
                totalRecords,
                nulls == null ? new boolean[totalRecords] : nulls,
                bytes == null ? new byte[totalRecords] : bytes);
    }
}
