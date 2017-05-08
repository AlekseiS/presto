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
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Marker.Bound;
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
import java.util.Optional;

import static com.facebook.presto.connector.thrift.api.PrestoThriftRangeValueSet.PrestoThriftBound.fromBound;
import static com.facebook.presto.connector.thrift.api.PrestoThriftRangeValueSet.PrestoThriftMarker.fromMarker;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftRangeValueSet
{
    private final List<PrestoThriftRange> ranges;

    @ThriftConstructor
    public PrestoThriftRangeValueSet(@ThriftField(name = "ranges") List<PrestoThriftRange> ranges)
    {
        this.ranges = ranges;
    }

    @ThriftField(1)
    public List<PrestoThriftRange> getRanges()
    {
        return ranges;
    }

    public SortedRangeSet toRangeValueSet(Type type)
    {
        List<Range> result = new ArrayList<>(ranges.size());
        for (PrestoThriftRange thriftRange : ranges) {
            result.add(thriftRange.toRange(type));
        }
        return SortedRangeSet.copyOf(type, result);
    }

    public static PrestoThriftRangeValueSet fromSortedRangeSet(SortedRangeSet valueSet)
    {
        List<PrestoThriftRange> ranges = valueSet.getOrderedRanges()
                .stream()
                .map(PrestoThriftRange::fromRange)
                .collect(toImmutableList());
        return new PrestoThriftRangeValueSet(ranges);
    }

    @ThriftEnum
    public enum PrestoThriftBound
    {
        BELOW(1),   // lower than the value, but infinitesimally close to the value
        EXACTLY(2), // exactly the value
        ABOVE(3);   // higher than the value, but infinitesimally close to the value

        private final int value;

        PrestoThriftBound(int value)
        {
            this.value = value;
        }

        @ThriftEnumValue
        public int getValue()
        {
            return value;
        }

        public Bound toMarkerBound()
        {
            switch (this) {
                case BELOW:
                    return Bound.BELOW;
                case EXACTLY:
                    return Bound.EXACTLY;
                case ABOVE:
                    return Bound.ABOVE;
                default:
                    throw new IllegalArgumentException("Unknown thrift bound: " + this);
            }
        }

        public static PrestoThriftBound fromBound(Bound bound)
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
    }

    /**
     * LOWER UNBOUNDED is specified with an empty value and a ABOVE bound
     * UPPER UNBOUNDED is specified with an empty value and a BELOW bound
     */
    @ThriftStruct
    public static final class PrestoThriftMarker
    {
        private final PrestoThriftColumnData value;
        private final PrestoThriftBound bound;

        @ThriftConstructor
        public PrestoThriftMarker(@Nullable PrestoThriftColumnData value, PrestoThriftBound bound)
        {
            checkArgument(value == null || value.numberOfRecords() == 1, "value must contain exactly one value when present");
            this.value = value;
            this.bound = requireNonNull(bound, "bound is null");
        }

        @Nullable
        @ThriftField(value = 1, requiredness = OPTIONAL)
        public PrestoThriftColumnData getValue()
        {
            return value;
        }

        @ThriftField(2)
        public PrestoThriftBound getBound()
        {
            return bound;
        }

        public Marker toMarker(Type type)
        {
            if (value == null) {
                switch (bound) {
                    case ABOVE:
                        return Marker.lowerUnbounded(type);
                    case BELOW:
                        return Marker.upperUnbounded(type);
                    case EXACTLY:
                        throw new IllegalArgumentException("Value cannot be null for 'EXACTLY' bound");
                    default:
                        throw new IllegalArgumentException("Unknown bound type: " + bound);
                }
            }
            else {
                return new Marker(type, Optional.of(value.toBlock(type)), bound.toMarkerBound());
            }
        }

        public static PrestoThriftMarker fromMarker(Marker marker)
        {
            PrestoThriftColumnData value;
            if (!marker.getValueBlock().isPresent()) {
                value = null;
            }
            else {
                ColumnBuilder builder = PrestoThriftColumnData.builder(marker.getType(), 1);
                builder.append(marker.getValueBlock().get(), 0, marker.getType());
                value = builder.build();
            }
            return new PrestoThriftMarker(value, fromBound(marker.getBound()));
        }
    }

    @ThriftStruct
    public static final class PrestoThriftRange
    {
        private final PrestoThriftMarker low;
        private final PrestoThriftMarker high;

        @ThriftConstructor
        public PrestoThriftRange(PrestoThriftMarker low, PrestoThriftMarker high)
        {
            this.low = requireNonNull(low, "low is null");
            this.high = requireNonNull(high, "high is null");
        }

        @ThriftField(1)
        public PrestoThriftMarker getLow()
        {
            return low;
        }

        @ThriftField(2)
        public PrestoThriftMarker getHigh()
        {
            return high;
        }

        public Range toRange(Type type)
        {
            return new Range(low.toMarker(type), high.toMarker(type));
        }

        public static PrestoThriftRange fromRange(Range range)
        {
            return new PrestoThriftRange(fromMarker(range.getLow()), fromMarker(range.getHigh()));
        }
    }
}
