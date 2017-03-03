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
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.genericthrift.client.ThriftRangeValueSet.ThriftRange.Bound.toMarkerBound;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class ThriftSingleValue
{
    private final List<ThriftColumnData> columnsData;

    @ThriftConstructor
    public ThriftSingleValue(List<ThriftColumnData> columnsData)
    {
        this.columnsData = requireNonNull(columnsData, "columnsData is null");
        checkArgument(!columnsData.isEmpty(), "Columns data is empty");
    }

    @ThriftField(1)
    public List<ThriftColumnData> getColumnsData()
    {
        return columnsData;
    }

    @Nullable
    public static ThriftSingleValue fromMarker(Marker marker)
    {
        if (!marker.getValueBlock().isPresent()) {
            return null;
        }
        return withValue(marker.getValue(), marker.getType());
    }

    public static Marker toMarker(@Nullable ThriftSingleValue value, Type type, ThriftRangeValueSet.ThriftRange.Bound bound)
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
            ColumnReader reader = ColumnReaders.createColumnReader(value.getColumnsData(), "value", type, 1);
            return new Marker(type, Optional.of(reader.readBlock(1)), toMarkerBound(bound));
        }
    }

    private static ThriftSingleValue withValue(Object value, Type type)
    {
        return new ThriftSingleValue(ImmutableList.of(new ThriftColumnData.Builder().setValue(0, value, type).setColumnName("value").build()));
    }
}
