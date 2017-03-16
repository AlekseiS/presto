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
package com.facebook.presto.genericthrift.node.utils;

import com.facebook.presto.genericthrift.client.ThriftRowsBatch;
import com.facebook.presto.genericthrift.readers.ColumnReader;
import com.facebook.presto.genericthrift.readers.ColumnReaders;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class WrappingRecordSet
        implements RecordSet
{
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final ThriftRowsBatch keys;

    public WrappingRecordSet(ThriftRowsBatch keys, List<String> columnNames, List<Type> columnTypes)
    {
        this.columnNames = requireNonNull(columnNames, "columnNames is null");
        this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "names and types lists must be of the same length");
        this.keys = requireNonNull(keys, "keys is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        List<ColumnReader> readers = new ArrayList<>(columnTypes.size());
        for (int i = 0; i < columnNames.size(); i++) {
            readers.add(ColumnReaders.create(keys.getColumnsData(), columnNames.get(i), columnTypes.get(i), keys.getRowCount()));
        }
        return new WrappingCursor(readers, columnTypes);
    }

    private static class WrappingCursor
            implements RecordCursor
    {
        private final List<ColumnReader> readers;
        private final List<Type> columnTypes;
        private final Block[] blocks;

        public WrappingCursor(List<ColumnReader> readers, List<Type> columnTypes)
        {
            this.readers = requireNonNull(readers, "readers is null");
            this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
            checkArgument(readers.size() == columnTypes.size(), "number of readers and columns must match");
            checkArgument(!readers.isEmpty(), "at least one reader must be present");
            blocks = new Block[readers.size()];
        }

        @Override
        public long getTotalBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return columnTypes.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (!readers.get(0).hasMoreRecords()) {
                return false;
            }
            int idx = 0;
            for (ColumnReader reader : readers) {
                checkState(reader.hasMoreRecords(), "all readers must have the same number of records");
                blocks[idx] = reader.readBlock(1);
                idx++;
            }
            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return columnTypes.get(field).getBoolean(blocks[field], 0);
        }

        @Override
        public long getLong(int field)
        {
            return columnTypes.get(field).getLong(blocks[field], 0);
        }

        @Override
        public double getDouble(int field)
        {
            return columnTypes.get(field).getDouble(blocks[field], 0);
        }

        @Override
        public Slice getSlice(int field)
        {
            return columnTypes.get(field).getSlice(blocks[field], 0);
        }

        @Override
        public Object getObject(int field)
        {
            return columnTypes.get(field).getObject(blocks[field], 0);
        }

        @Override
        public boolean isNull(int field)
        {
            return blocks[field].isNull(0);
        }

        @Override
        public void close()
        {
        }
    }
}
