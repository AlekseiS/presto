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
package com.facebook.presto.thrift.interfaces.client;

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

import static com.facebook.presto.thrift.interfaces.client.ThriftAllOrNoneValueSet.fromAllOrNoneValueSet;
import static com.facebook.presto.thrift.interfaces.client.ThriftAllOrNoneValueSet.toAllOrNoneValueSet;
import static com.facebook.presto.thrift.interfaces.client.ThriftEquatableValueSet.fromEquatableValueSet;
import static com.facebook.presto.thrift.interfaces.client.ThriftEquatableValueSet.toEquatableValueSet;
import static com.facebook.presto.thrift.interfaces.client.ThriftRangeValueSet.fromSortedRangeSet;
import static com.facebook.presto.thrift.interfaces.client.ThriftRangeValueSet.toRangeValueSet;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkArgument;

@ThriftStruct
public final class ThriftDomain
{
    private final ThriftAllOrNoneValueSet allOrNoneValueSet;
    private final ThriftEquatableValueSet equatableValueSet;
    private final ThriftRangeValueSet rangeValueSet;
    private final boolean nullAllowed;

    @ThriftConstructor
    public ThriftDomain(
            @Nullable @ThriftField(name = "allOrNoneValueSet") ThriftAllOrNoneValueSet allOrNoneValueSet,
            @Nullable @ThriftField(name = "equatableValueSet") ThriftEquatableValueSet equatableValueSet,
            @Nullable @ThriftField(name = "rangeValueSet") ThriftRangeValueSet rangeValueSet,
            @ThriftField(name = "nullAllowed") boolean nullAllowed)
    {
        this.allOrNoneValueSet = allOrNoneValueSet;
        this.equatableValueSet = equatableValueSet;
        this.rangeValueSet = rangeValueSet;
        this.nullAllowed = nullAllowed;
        checkArgument(isExactlyOneNonNull(allOrNoneValueSet, equatableValueSet, rangeValueSet), "Exactly one value set must be set");
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

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public ThriftAllOrNoneValueSet getAllOrNoneValueSet()
    {
        return allOrNoneValueSet;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public ThriftEquatableValueSet getEquatableValueSet()
    {
        return equatableValueSet;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public ThriftRangeValueSet getRangeValueSet()
    {
        return rangeValueSet;
    }

    @ThriftField(4)
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    public static ThriftDomain fromDomain(Domain domain)
    {
        ValueSet valueSet = domain.getValues();
        if (valueSet.getClass() == AllOrNoneValueSet.class) {
            return new ThriftDomain(
                    fromAllOrNoneValueSet((AllOrNoneValueSet) valueSet),
                    null,
                    null,
                    domain.isNullAllowed());
        }
        else if (valueSet.getClass() == EquatableValueSet.class) {
            return new ThriftDomain(
                    null,
                    fromEquatableValueSet((EquatableValueSet) valueSet),
                    null,
                    domain.isNullAllowed());
        }
        else if (valueSet.getClass() == SortedRangeSet.class) {
            return new ThriftDomain(
                    null,
                    null,
                    fromSortedRangeSet((SortedRangeSet) valueSet),
                    domain.isNullAllowed());
        }
        else {
            throw new IllegalArgumentException("Unknown implementation of a value set: " + valueSet.getClass());
        }
    }

    public static Domain toDomain(ThriftDomain thriftDomain, Type columnType)
    {
        if (thriftDomain.getAllOrNoneValueSet() != null) {
            return Domain.create(
                    toAllOrNoneValueSet(thriftDomain.getAllOrNoneValueSet(), columnType),
                    thriftDomain.isNullAllowed());
        }
        else if (thriftDomain.getEquatableValueSet() != null) {
            return Domain.create(
                    toEquatableValueSet(thriftDomain.getEquatableValueSet(), columnType),
                    thriftDomain.isNullAllowed());
        }
        else if (thriftDomain.getRangeValueSet() != null) {
            return Domain.create(
                    toRangeValueSet(thriftDomain.getRangeValueSet(), columnType),
                    thriftDomain.isNullAllowed());
        }
        else {
            throw new IllegalArgumentException("Unknown value set used in thrift structure");
        }
    }
}
