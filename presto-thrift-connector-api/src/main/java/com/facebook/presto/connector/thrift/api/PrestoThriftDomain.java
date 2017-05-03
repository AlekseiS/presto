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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import static com.facebook.presto.connector.thrift.api.PrestoThriftAllOrNoneValueSet.fromAllOrNoneValueSet;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkArgument;

@ThriftStruct
public final class PrestoThriftDomain
{
    private final PrestoThriftAllOrNoneValueSet allOrNoneValueSet;
    private final PrestoThriftEquatableValueSet equatableValueSet;
    private final PrestoThriftRangeValueSet rangeValueSet;
    private final boolean nullAllowed;

    @ThriftConstructor
    public PrestoThriftDomain(
            @Nullable PrestoThriftAllOrNoneValueSet allOrNoneValueSet,
            @Nullable PrestoThriftEquatableValueSet equatableValueSet,
            @Nullable PrestoThriftRangeValueSet rangeValueSet,
            boolean nullAllowed)
    {
        this.allOrNoneValueSet = allOrNoneValueSet;
        this.equatableValueSet = equatableValueSet;
        this.rangeValueSet = rangeValueSet;
        this.nullAllowed = nullAllowed;
        checkArgument(isExactlyOneNonNull(allOrNoneValueSet, equatableValueSet, rangeValueSet), "Exactly one value set must be set");
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

    @ThriftField(4)
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    public Domain toDomain(Type columnType)
    {
        if (allOrNoneValueSet != null) {
            return Domain.create(
                    allOrNoneValueSet.toAllOrNoneValueSet(columnType),
                    nullAllowed);
        }
        else if (equatableValueSet != null) {
            return Domain.create(
                    equatableValueSet.toEquatableValueSet(columnType),
                    nullAllowed);
        }
        else if (rangeValueSet != null) {
            return Domain.create(
                    rangeValueSet.toRangeValueSet(columnType),
                    nullAllowed);
        }
        else {
            throw new IllegalArgumentException("Unknown value set used in thrift structure");
        }
    }

    public static PrestoThriftDomain fromDomain(Domain domain)
    {
        ValueSet valueSet = domain.getValues();
        if (valueSet.getClass() == AllOrNoneValueSet.class) {
            return new PrestoThriftDomain(
                    fromAllOrNoneValueSet((AllOrNoneValueSet) valueSet),
                    null,
                    null,
                    domain.isNullAllowed());
        }
        else if (valueSet.getClass() == EquatableValueSet.class) {
            return new PrestoThriftDomain(
                    null,
                    PrestoThriftEquatableValueSet.fromEquatableValueSet((EquatableValueSet) valueSet),
                    null,
                    domain.isNullAllowed());
        }
        else if (valueSet.getClass() == SortedRangeSet.class) {
            return new PrestoThriftDomain(
                    null,
                    null,
                    PrestoThriftRangeValueSet.fromSortedRangeSet((SortedRangeSet) valueSet),
                    domain.isNullAllowed());
        }
        else {
            throw new IllegalArgumentException("Unknown implementation of a value set: " + valueSet.getClass());
        }
    }

    private static boolean isExactlyOneNonNull(Object... values)
    {
        int sum = 0;
        for (Object value : values) {
            if (value != null) {
                sum++;
            }
        }
        return sum == 1;
    }
}
