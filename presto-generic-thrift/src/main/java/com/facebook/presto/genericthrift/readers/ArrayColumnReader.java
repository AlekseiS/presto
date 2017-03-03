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
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class ArrayColumnReader
        implements ColumnReader
{
    private final SizeColumnReader sizeReader;
    private final ColumnReader elementReader;
    private final int totalRecords;
    private int idx;

    public ArrayColumnReader(SizeColumnReader sizeReader, ColumnReader elementReader, int totalRecords)
    {
        this.sizeReader = requireNonNull(sizeReader, "sizeReader is null");
        this.elementReader = requireNonNull(elementReader, "elementReader is null");
        checkArgument(totalRecords >= 0, "totalRecords is negative");
        this.totalRecords = totalRecords;
    }

    @Override
    public Block readBlock(int nextBatchSize)
    {
        int end = min(idx + nextBatchSize, totalRecords);
        int numElements = sizeReader.sum(idx, end);
        Block block = elementReader.readBlock(numElements);
        int size = end - idx;
        boolean[] valueIsNull = sizeReader.nullValues(idx, end);
        int[] offsets = new int[size + 1];
        for (int i = 1; i < offsets.length; i++) {
            offsets[i] = offsets[i - 1] + sizeReader.get(i - 1);
        }
        idx = end;
        return new ArrayBlock(size, valueIsNull, offsets, block);
    }

    @Override
    public boolean hasMoreRecords()
    {
        return idx < totalRecords;
    }

    public static ArrayColumnReader createReader(List<ThriftColumnData> columnsData, String columnName, Type type, int totalRecords)
    {
        SizeColumnReader sizeReader = SizeColumnReader.createReader(columnsData, columnName, IntegerType.INTEGER, totalRecords);
        int expectedRecords = sizeReader.sum();
        ColumnReader elementsReader = ColumnReaders.createColumnReader(columnsData, columnName + ".e", type.getTypeParameters().get(0), expectedRecords);
        return new ArrayColumnReader(sizeReader, elementsReader, totalRecords);
    }
}
