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
package com.facebook.presto.connector.thrift.writers;

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnData;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ArrayColumnWriter
        implements ColumnWriter
{
    private final Type elementType;
    private final IntColumnWriter sizeWriter;
    private final ColumnWriter elementWriter;

    public ArrayColumnWriter(String columnName, int initialCapacity, Type elementType)
    {
        this.elementType = requireNonNull(elementType, "elementType is null");
        this.sizeWriter = new IntColumnWriter(columnName, initialCapacity);
        this.elementWriter = ColumnWriters.create(columnName + ".e", elementType, initialCapacity);
    }

    @Override
    public void append(RecordCursor cursor, int field)
    {
        throw new UnsupportedOperationException("Writing array type from record cursor is not supported");
    }

    @Override
    public void append(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            sizeWriter.appendNull();
        }
        else {
            // array value must be a block
            Block value = (Block) type.getObject(block, position);
            sizeWriter.appendValue(value.getPositionCount());
            appendAllValues(value);
        }
    }

    // this method can potentially be a part of writer interface which can make allocations more efficient
    private void appendAllValues(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementWriter.append(block, i, elementType);
        }
    }

    @Override
    public List<PrestoThriftColumnData> getResult()
    {
        return ImmutableList.<PrestoThriftColumnData>builder()
                .addAll(sizeWriter.getResult())
                .addAll(elementWriter.getResult())
                .build();
    }
}
