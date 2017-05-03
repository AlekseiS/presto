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
import com.facebook.presto.connector.thrift.readers.IntColumnReader.NullsAndOffsets;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.util.List;

public final class ArrayColumnReader
{
    private ArrayColumnReader()
    {
    }

    public static Block readBlock(List<PrestoThriftColumnData> columnsData, String columnName, Type columnType, int totalRecords)
    {
        NullsAndOffsets nullsAndOffsets = IntColumnReader.readNullsAndOffsets(columnsData, columnName, totalRecords);
        int expectedRecords = nullsAndOffsets.getOffsets()[nullsAndOffsets.getOffsets().length - 1];
        Block elements = ColumnReaders.readBlock(columnsData, columnName + ".e", columnType.getTypeParameters().get(0), expectedRecords);
        return new ArrayBlock(totalRecords, nullsAndOffsets.getNulls(), nullsAndOffsets.getOffsets(), elements);
    }
}
