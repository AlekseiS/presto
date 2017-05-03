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
import com.facebook.presto.spi.predicate.EquatableValueSet.ValueEntry;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.JsonType.JSON;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPrestoThriftDomain
{
    @Test
    public void testDomainSimple()
            throws Exception
    {
        testConversion(Domain.all(BIGINT));
        testConversion(Domain.none(INTEGER));
        testConversion(Domain.notNull(DOUBLE));
        testConversion(Domain.onlyNull(VARCHAR));
        // comparable and orderable
        testConversion(Domain.singleValue(INTEGER, 21L));
        // comparable, but not orderable
        testConversion(Domain.singleValue(JSON, utf8Slice("\"key1\":\"value1\"")));
    }

    @Test
    public void testDomainAllOrNoneSet()
            throws Exception
    {
        testConversion(Domain.create(new AllOrNoneValueSet(BIGINT, true), false));
        testConversion(Domain.create(new AllOrNoneValueSet(VARCHAR, false), true));
    }

    @Test
    public void testDomainEquatableSet()
            throws Exception
    {
        // equatable value set is used only with comparable, but not orderable types
        testConversion(Domain.create(new EquatableValueSet(JSON, false, ImmutableSet.of()), true));
        testConversion(Domain.create(new EquatableValueSet(JSON, true, ImmutableSet.of(
                ValueEntry.create(JSON, utf8Slice("\"key1\":\"value1\"")),
                ValueEntry.create(JSON, utf8Slice("\"key2\":\"value2\"")),
                ValueEntry.create(JSON, utf8Slice("\"key3\":\"value3\""))
        )), false));
        testConversion(Domain.create(new EquatableValueSet(JSON, false, ImmutableSet.of(
                ValueEntry.create(JSON, utf8Slice("\"key1\":\"value1\"")),
                ValueEntry.create(JSON, utf8Slice("\"key2\":\"value2\"")),
                ValueEntry.create(JSON, utf8Slice("\"key3\":\"value3\""))
        )), true));
    }

    @Test
    public void testDomainRangeSet()
            throws Exception
    {
        // sorted range set is used only with orderable and comparable types
        testConversion(Domain.create(SortedRangeSet.copyOf(INTEGER, ImmutableList.of()), true));
        testConversion(Domain.create(SortedRangeSet.copyOf(BIGINT,
                ImmutableList.of(Range.greaterThanOrEqual(BIGINT, -10L), Range.lessThan(BIGINT, 101L))
        ), false));
        testConversion(Domain.create(SortedRangeSet.copyOf(TINYINT,
                ImmutableList.of(Range.equal(TINYINT, 0L), Range.equal(TINYINT, -1L), Range.greaterThan(TINYINT, 1L))
        ), true));
    }

    private static void testConversion(Domain domain)
    {
        Type type = domain.getType();
        PrestoThriftDomain converted = PrestoThriftDomain.fromDomain(domain);
        assertNotNull(converted);
        Domain actual = converted.toDomain(type);
        assertEquals(actual, domain);
    }
}
