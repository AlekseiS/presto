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

import com.facebook.presto.connector.thrift.api.builders.ColumnBuilder;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.predicate.EquatableValueSet.ValueEntry;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftEquatableValueSet
{
    private final boolean whiteList;
    private final PrestoThriftColumnData values;

    @ThriftConstructor
    public PrestoThriftEquatableValueSet(boolean whiteList, PrestoThriftColumnData values)
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
    public PrestoThriftColumnData getValues()
    {
        return values;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(whiteList, values);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftEquatableValueSet other = (PrestoThriftEquatableValueSet) obj;
        return this.whiteList == other.whiteList &&
                Objects.equals(this.values, other.values);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("whiteList", whiteList)
                .add("valuesClass", values.getClass())
                .toString();
    }

    public static PrestoThriftEquatableValueSet fromEquatableValueSet(EquatableValueSet valueSet)
    {
        Type type = valueSet.getType();
        Set<ValueEntry> values = valueSet.getEntries();
        ColumnBuilder builder = PrestoThriftColumnData.builder(type, values.size());
        for (ValueEntry value : values) {
            checkState(type.equals(value.getType()), "ValueEntrySet has elements of different types: %s vs %s", type, value.getType());
            builder.append(value.getBlock(), 0, type);
        }
        return new PrestoThriftEquatableValueSet(valueSet.isWhiteList(), builder.build());
    }
}
