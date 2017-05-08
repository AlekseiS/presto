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

import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftEnum;
import com.facebook.swift.codec.ThriftEnumValue;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.connector.thrift.api.PrestoThriftRangeValueSet.ThriftRange.Bound.fromMarkerBound;
import static com.facebook.presto.connector.thrift.api.PrestoThriftSingleValue.fromMarker;
import static com.facebook.presto.connector.thrift.api.PrestoThriftSingleValue.toMarker;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.collect.ImmutableList.toImmutableList;

@ThriftStruct
public final class PrestoThriftRangeValueSet
{
    private final List<ThriftRange> ranges;

    @ThriftConstructor
    public PrestoThriftRangeValueSet(@ThriftField(name = "ranges") List<ThriftRange> ranges)
    {
        this.ranges = ranges;
    }

    @ThriftField(1)
    public List<ThriftRange> getRanges()
    {
        return ranges;
    }

    public SortedRangeSet toRangeValueSet(Type type)
    {
        List<Range> result = new ArrayList<>(ranges.size());
        for (ThriftRange thriftRange : ranges) {
            result.add(thriftRange.toRange(type));
        }
        return SortedRangeSet.copyOf(type, result);
    }

    public static PrestoThriftRangeValueSet fromSortedRangeSet(SortedRangeSet valueSet)
    {
        List<ThriftRange> ranges = valueSet.getOrderedRanges()
                .stream()
                .map(ThriftRange::fromRange)
                .collect(toImmutableList());
        return new PrestoThriftRangeValueSet(ranges);
    }

    @ThriftStruct
    public static final class ThriftRange
    {
        @ThriftEnum
        public enum Bound
        {
            BELOW(1),   // lower than the value, but infinitesimally close to the value
            EXACTLY(2), // exactly the value
            ABOVE(3);   // higher than the value, but infinitesimally close to the value

            private final int value;

            Bound(int value)
            {
                this.value = value;
            }

            @ThriftEnumValue
            public int getValue()
            {
                return value;
            }

            public static Bound fromMarkerBound(Marker.Bound bound)
            {
                switch (bound) {
                    case BELOW:
                        return BELOW;
                    case EXACTLY:
                        return EXACTLY;
                    case ABOVE:
                        return ABOVE;
                    default:
                        throw new IllegalArgumentException("Unknown bound: " + bound);
                }
            }

            public static Marker.Bound toMarkerBound(Bound thriftBound)
            {
                switch (thriftBound) {
                    case BELOW:
                        return Marker.Bound.BELOW;
                    case EXACTLY:
                        return Marker.Bound.EXACTLY;
                    case ABOVE:
                        return Marker.Bound.ABOVE;
                    default:
                        throw new IllegalArgumentException("Unknown thrift bound: " + thriftBound);
                }
            }
        }

        private final PrestoThriftSingleValue low;
        private final Bound lowerBound;
        private final PrestoThriftSingleValue high;
        private final Bound upperBound;

        @ThriftConstructor
        public ThriftRange(
                @Nullable PrestoThriftSingleValue low,
                Bound lowerBound,
                @Nullable PrestoThriftSingleValue high,
                Bound upperBound)
        {
            this.low = low;
            this.lowerBound = lowerBound;
            this.high = high;
            this.upperBound = upperBound;
        }

        @Nullable
        @ThriftField(value = 1, requiredness = OPTIONAL)
        public PrestoThriftSingleValue getLow()
        {
            return low;
        }

        @ThriftField(2)
        public Bound getLowerBound()
        {
            return lowerBound;
        }

        @Nullable
        @ThriftField(value = 3, requiredness = OPTIONAL)
        public PrestoThriftSingleValue getHigh()
        {
            return high;
        }

        @ThriftField(4)
        public Bound getUpperBound()
        {
            return upperBound;
        }

        public Range toRange(Type type)
        {
            return new Range(
                    toMarker(low, type, lowerBound),
                    toMarker(high, type, upperBound));
        }

        public static ThriftRange fromRange(Range range)
        {
            return new ThriftRange(
                    fromMarker(range.getLow()),
                    fromMarkerBound(range.getLow().getBound()),
                    fromMarker(range.getHigh()),
                    fromMarkerBound(range.getHigh().getBound()));
        }
    }
}
