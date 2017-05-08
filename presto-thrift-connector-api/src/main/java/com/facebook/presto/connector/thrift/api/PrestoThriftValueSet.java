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

import com.facebook.presto.spi.predicate.AllOrNoneValueSet;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import static com.facebook.presto.connector.thrift.api.PrestoThriftAllOrNoneValueSet.fromAllOrNoneValueSet;
import static com.facebook.presto.connector.thrift.api.PrestoThriftEquatableValueSet.fromEquatableValueSet;
import static com.facebook.presto.connector.thrift.api.PrestoThriftRangeValueSet.fromSortedRangeSet;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkArgument;

@ThriftStruct
public final class PrestoThriftValueSet
{
    private final PrestoThriftAllOrNoneValueSet allOrNoneValueSet;
    private final PrestoThriftEquatableValueSet equatableValueSet;
    private final PrestoThriftRangeValueSet rangeValueSet;

    @ThriftConstructor
    public PrestoThriftValueSet(
            @Nullable PrestoThriftAllOrNoneValueSet allOrNoneValueSet,
            @Nullable PrestoThriftEquatableValueSet equatableValueSet,
            @Nullable PrestoThriftRangeValueSet rangeValueSet)
    {
        checkArgument(isExactlyOneNonNull(allOrNoneValueSet, equatableValueSet, rangeValueSet), "exactly one value set must be present");
        this.allOrNoneValueSet = allOrNoneValueSet;
        this.equatableValueSet = equatableValueSet;
        this.rangeValueSet = rangeValueSet;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public PrestoThriftAllOrNoneValueSet getAllOrNoneValueSet()
    {
        return allOrNoneValueSet;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public PrestoThriftEquatableValueSet getEquatableValueSet()
    {
        return equatableValueSet;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public PrestoThriftRangeValueSet getRangeValueSet()
    {
        return rangeValueSet;
    }

    public static PrestoThriftValueSet fromValueSet(ValueSet valueSet)
    {
        if (valueSet.getClass() == AllOrNoneValueSet.class) {
            return new PrestoThriftValueSet(
                    fromAllOrNoneValueSet((AllOrNoneValueSet) valueSet),
                    null,
                    null);
        }
        else if (valueSet.getClass() == EquatableValueSet.class) {
            return new PrestoThriftValueSet(
                    null,
                    fromEquatableValueSet((EquatableValueSet) valueSet),
                    null);
        }
        else if (valueSet.getClass() == SortedRangeSet.class) {
            return new PrestoThriftValueSet(
                    null,
                    null,
                    fromSortedRangeSet((SortedRangeSet) valueSet));
        }
        else {
            throw new IllegalArgumentException("Unknown implementation of a value set: " + valueSet.getClass());
        }
    }

    private static boolean isExactlyOneNonNull(Object a, Object b, Object c)
    {
        return a != null && b == null && c == null ||
                a == null && b != null && c == null ||
                a == null && b == null && c != null;
    }
}
