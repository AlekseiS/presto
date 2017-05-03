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

import com.facebook.presto.connector.thrift.readers.ColumnReaders;
import com.facebook.presto.connector.thrift.writers.ColumnWriter;
import com.facebook.presto.connector.thrift.writers.ColumnWriters;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftSingleValue
{
    public static final String VALUE_COLUMN_NAME = "value";
    private final List<PrestoThriftColumnData> columnsData;

    @ThriftConstructor
    public PrestoThriftSingleValue(List<PrestoThriftColumnData> columnsData)
    {
        this.columnsData = requireNonNull(columnsData, "columnsData is null");
        checkArgument(!columnsData.isEmpty(), "Columns data is empty");
    }

    @ThriftField(1)
    public List<PrestoThriftColumnData> getColumnsData()
    {
        return columnsData;
    }

    @Nullable
    public static PrestoThriftSingleValue fromMarker(Marker marker)
    {
        if (!marker.getValueBlock().isPresent()) {
            return null;
        }
        ColumnWriter writer = ColumnWriters.create(VALUE_COLUMN_NAME, marker.getType(), 1);
        Block valueBlock = marker.getValueBlock().get();
        checkState(valueBlock.getPositionCount() == 1,
                "Block in Marker has more than one position: %s", valueBlock.getPositionCount());
        writer.append(valueBlock, 0, marker.getType());
        return new PrestoThriftSingleValue(writer.getResult());
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
            Block block = ColumnReaders.readBlock(value.getColumnsData(), VALUE_COLUMN_NAME, type, 1);
            return new Marker(type, Optional.of(block), PrestoThriftRangeValueSet.ThriftRange.Bound.toMarkerBound(bound));
        }
    }
}
