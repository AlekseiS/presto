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
package com.facebook.presto.connector.thrift.api;

import com.facebook.presto.connector.thrift.readers.ColumnReaders;
import com.facebook.presto.connector.thrift.writers.ColumnWriter;
import com.facebook.presto.connector.thrift.writers.ColumnWriters;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.connector.thrift.api.PrestoThriftEquatableValueSet.ThriftValueEntrySet.fromValueEntries;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftEquatableValueSet
{
    private final boolean whiteList;
    private final ThriftValueEntrySet values;

    @ThriftConstructor
    public PrestoThriftEquatableValueSet(boolean whiteList, ThriftValueEntrySet values)
    {
        this.whiteList = whiteList;
        this.values = requireNonNull(values, "values are null");
    }

    @ThriftField(1)
    public boolean isWhiteList()
    {
        return whiteList;
    }

    @ThriftField(2)
    public ThriftValueEntrySet getValues()
    {
        return values;
    }

    public EquatableValueSet toEquatableValueSet(Type type)
    {
        return new EquatableValueSet(type, whiteList, values.toValueEntries(type));
    }

    public static PrestoThriftEquatableValueSet fromEquatableValueSet(EquatableValueSet valueSet)
    {
        return new PrestoThriftEquatableValueSet(valueSet.isWhiteList(), fromValueEntries(valueSet.getEntries()));
    }

    @ThriftStruct
    public static final class ThriftValueEntrySet
    {
        private final List<PrestoThriftColumnData> columnsData;
        private final int elementCount;

        @ThriftConstructor
        public ThriftValueEntrySet(List<PrestoThriftColumnData> columnsData, int elementCount)
        {
            this.columnsData = requireNonNull(columnsData, "columnData is null");
            checkArgument(elementCount >= 0, "elementCount is negative");
            this.elementCount = elementCount;
        }

        @ThriftField(1)
        public List<PrestoThriftColumnData> getColumnsData()
        {
            return columnsData;
        }

        @ThriftField(2)
        public int getElementCount()
        {
            return elementCount;
        }

        public Set<EquatableValueSet.ValueEntry> toValueEntries(Type type)
        {
            if (elementCount == 0) {
                return ImmutableSet.of();
            }
            Block block = ColumnReaders.readBlock(columnsData, "value", type, elementCount);
            Set<EquatableValueSet.ValueEntry> result = new HashSet<>(elementCount);
            for (int i = 0; i < elementCount; i++) {
                result.add(new EquatableValueSet.ValueEntry(type, block.copyRegion(i, 1)));
            }
            return unmodifiableSet(result);
        }

        public static ThriftValueEntrySet fromValueEntries(Set<EquatableValueSet.ValueEntry> values)
        {
            if (values.isEmpty()) {
                return new ThriftValueEntrySet(ImmutableList.of(), 0);
            }
            Type type = values.iterator().next().getType();
            ColumnWriter writer = ColumnWriters.create("value", type, values.size());
            int idx = 0;
            for (EquatableValueSet.ValueEntry value : values) {
                checkState(type.equals(value.getType()),
                        "ValueEntrySet has elements of different types: %s vs %s", type, value.getType());
                Block valueBlock = value.getBlock();
                checkState(valueBlock.getPositionCount() == 1,
                        "Block in ValueEntry has more than one position: %s", valueBlock.getPositionCount());
                writer.append(valueBlock, 0, type);
                idx++;
            }
            return new ThriftValueEntrySet(writer.getResult(), idx);
        }
    }
}
