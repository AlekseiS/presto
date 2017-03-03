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
        private final int count;

        @ThriftConstructor
        public ThriftValueEntrySet(List<ThriftColumnData> columnsData, int count)
        {
            this.columnsData = requireNonNull(columnsData, "columnData is null");
            checkArgument(count >= 0, "count is negative");
            this.count = count;
        }

        @ThriftField(1)
        public List<ThriftColumnData> getColumnsData()
        {
            return columnsData;
        }

        @ThriftField(2)
        public int getCount()
        {
            return count;
        }

        public static ThriftValueEntrySet fromValueEntries(Set<EquatableValueSet.ValueEntry> values)
        {
            ThriftColumnData.Builder builder = new ThriftColumnData.Builder();
            int idx = 0;
            for (EquatableValueSet.ValueEntry value : values) {
                builder.setValue(idx, value.getValue(), value.getType());
                idx++;
            }
            builder.setColumnName("value");
            return new ThriftValueEntrySet(ImmutableList.of(builder.build()), idx);
        }

        public static Set<EquatableValueSet.ValueEntry> toValueEntries(ThriftValueEntrySet values, Type type)
        {
            ColumnReader reader = ColumnReaders.createColumnReader(values.getColumnsData(), "value", type, values.getCount());
            Set<EquatableValueSet.ValueEntry> result = new HashSet<>(values.getCount());
            for (int i = 0; i < values.getCount(); i++) {
                result.add(new EquatableValueSet.ValueEntry(type, reader.readBlock(1)));
            }
            return unmodifiableSet(result);
        }
    }
}
