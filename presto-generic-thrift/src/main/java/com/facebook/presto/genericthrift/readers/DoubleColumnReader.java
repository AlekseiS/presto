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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class DoubleColumnReader
        implements ColumnReader
{
    private final Type type;
    private final boolean[] nulls;
    private final double[] doubles;
    private final int totalRecords;
    private int idx;

    public DoubleColumnReader(Type type, boolean[] nulls, double[] doubles, int totalRecords)
    {
        checkArgument(totalRecords >= 0, "totalRecords must be non-negative");
        checkArgument(totalRecords == 0 || nulls != null || doubles != null, "nulls array or values array must be present");
        checkArgument(nulls == null || nulls.length == totalRecords, "nulls must be null or of the expected size");
        checkArgument(doubles == null || doubles.length == totalRecords, "doubles must be null or of the expected size");
        this.type = requireNonNull(type, "type must be not null");
        this.nulls = nulls;
        this.doubles = doubles;
        this.totalRecords = totalRecords;
    }

    @Override
    public Block readBlock(int nextBatchSize)
    {
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), nextBatchSize);
        int end = min(idx + nextBatchSize, totalRecords);
        while (idx < end) {
            if (nulls != null && nulls[idx]) {
                builder.appendNull();
            }
            else {
                type.writeDouble(builder, doubles[idx]);
            }
            idx++;
        }
        return builder.build();
    }

    @Override
    public boolean hasMoreRecords()
    {
        return idx < totalRecords;
    }

    public static DoubleColumnReader createReader(List<ThriftColumnData> columnsData, String columnName, Type type, int totalRecords)
    {
        requireNonNull(columnName, "columnName must be non-null");
        ThriftColumnData columnData = ReaderUtils.columnByName(columnsData, columnName);
        checkArgument(columnData.getBooleans() == null
                        && columnData.getLongs() == null
                        && columnData.getBytes() == null
                        && columnData.getSizes() == null,
                "Remaining value containers must be null");
        return new DoubleColumnReader(type, columnData.getNulls(), columnData.getDoubles(), totalRecords);
    }
}
