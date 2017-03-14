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
package com.facebook.presto.genericthrift.client;

import com.facebook.presto.genericthrift.readers.ColumnReader;
import com.facebook.presto.genericthrift.readers.ColumnReaders;
import com.facebook.presto.genericthrift.writers.ColumnWriter;
import com.facebook.presto.genericthrift.writers.ColumnWriters;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.genericthrift.client.ThriftEquatableValueSet.ThriftValueEntrySet.fromValueEntries;
import static com.facebook.presto.genericthrift.client.ThriftEquatableValueSet.ThriftValueEntrySet.toValueEntries;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class ThriftEquatableValueSet
{
    private final ThriftValueEntrySet values;

    @ThriftConstructor
    public ThriftEquatableValueSet(ThriftValueEntrySet values)
    {
        this.values = requireNonNull(values, "values are null");
    }

    @ThriftField(1)
    public ThriftValueEntrySet getValues()
    {
        return values;
    }

    public static EquatableValueSet toEquatableValueSet(ThriftEquatableValueSet valueSet, Type type)
    {
        return new EquatableValueSet(type, true, toValueEntries(valueSet.getValues(), type));
    }

    public static ThriftEquatableValueSet fromEquatableValueSet(EquatableValueSet valueSet)
    {
        return new ThriftEquatableValueSet(fromValueEntries(valueSet.getEntries()));
    }

    @ThriftStruct
    public static final class ThriftValueEntrySet
    {
        private final List<ThriftColumnData> columnsData;
        private final int elementCount;

        @ThriftConstructor
        public ThriftValueEntrySet(List<ThriftColumnData> columnsData, int elementCount)
        {
            this.columnsData = requireNonNull(columnsData, "columnData is null");
            checkArgument(elementCount >= 0, "elementCount is negative");
            this.elementCount = elementCount;
        }

        @ThriftField(1)
        public List<ThriftColumnData> getColumnsData()
        {
            return columnsData;
        }

        @ThriftField(2)
        public int getElementCount()
        {
            return elementCount;
        }

        public static ThriftValueEntrySet fromValueEntries(Set<EquatableValueSet.ValueEntry> values)
        {
            if (values.isEmpty()) {
                return new ThriftValueEntrySet(ImmutableList.of(ThriftColumnData.empty("value")), 0);
            }
            Type type = values.iterator().next().getType();
            ColumnWriter writer = ColumnWriters.create("value", type, values.size());
            int idx = 0;
            for (EquatableValueSet.ValueEntry value : values) {
                checkState(type == value.getType(),
                        "ValueEntrySet has elements of different types: %s vs %s", type, value.getType());
                Block valueBlock = value.getBlock();
                checkState(valueBlock.getPositionCount() == 1,
                        "Block in ValueEntry has more than one position: %s", valueBlock.getPositionCount());
                writer.append(valueBlock, 0, type);
                idx++;
            }
            return new ThriftValueEntrySet(writer.getResult(), idx);
        }

        public static Set<EquatableValueSet.ValueEntry> toValueEntries(ThriftValueEntrySet values, Type type)
        {
            ColumnReader reader = ColumnReaders.create(values.getColumnsData(), "value", type, values.getElementCount());
            Set<EquatableValueSet.ValueEntry> result = new HashSet<>(values.getElementCount());
            for (int i = 0; i < values.getElementCount(); i++) {
                result.add(new EquatableValueSet.ValueEntry(type, reader.readBlock(1)));
            }
            return unmodifiableSet(result);
        }
    }
}
