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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import java.util.Optional;

import static com.facebook.presto.connector.thrift.api.PrestoThriftRangeValueSet.ThriftRange.Bound.toMarkerBound;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftSingleValue
{
    private final PrestoThriftColumnData columnData;

    @ThriftConstructor
    public PrestoThriftSingleValue(PrestoThriftColumnData columnData)
    {
        this.columnData = requireNonNull(columnData, "columnData is null");
        checkArgument(columnData.numberOfRecords() == 1, "data must contain exactly one record");
    }

    @ThriftField(1)
    public PrestoThriftColumnData getColumnData()
    {
        return columnData;
    }

    @Nullable
    public static PrestoThriftSingleValue fromMarker(Marker marker)
    {
        if (!marker.getValueBlock().isPresent()) {
            return null;
        }
        ColumnBuilder writer = PrestoThriftColumnData.builder(marker.getType(), 1);
        Block valueBlock = marker.getValueBlock().get();
        checkState(valueBlock.getPositionCount() == 1,
                "Block in Marker has more than one position: %s", valueBlock.getPositionCount());
        writer.append(valueBlock, 0, marker.getType());
        return new PrestoThriftSingleValue(writer.build());
    }

    public static Marker toMarker(@Nullable PrestoThriftSingleValue value, Type type, PrestoThriftRangeValueSet.ThriftRange.Bound bound)
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
            return new Marker(type, Optional.of(value.getColumnData().toBlock(type)), toMarkerBound(bound));
        }
    }
}
