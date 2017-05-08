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
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBigint;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBoolean;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftColumnType;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftDate;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftDouble;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftHyperLogLog;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftInteger;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftJson;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftTimestamp;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftVarchar;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkState;

@ThriftStruct
public final class PrestoThriftColumnData
{
    private final PrestoThriftBigint bigintData;
    private final PrestoThriftTimestamp timestampData;
    private final PrestoThriftInteger integerData;
    private final PrestoThriftBoolean booleanData;
    private final PrestoThriftDouble doubleData;
    private final PrestoThriftVarchar varcharData;
    private final PrestoThriftHyperLogLog hyperLogLogData;
    private final PrestoThriftJson jsonData;
    private final PrestoThriftDate dateData;

    // non-thrift field which points to non-null data item
    private final PrestoThriftColumnType dataReference;

    @ThriftConstructor
    public PrestoThriftColumnData(
            @Nullable PrestoThriftBigint bigintData,
            @Nullable PrestoThriftTimestamp timestampData,
            @Nullable PrestoThriftInteger integerData,
            @Nullable PrestoThriftBoolean booleanData,
            @Nullable PrestoThriftDouble doubleData,
            @Nullable PrestoThriftVarchar varcharData,
            @Nullable PrestoThriftHyperLogLog hyperLogLogData,
            @Nullable PrestoThriftJson jsonData,
            @Nullable PrestoThriftDate dateData)
    {
        this.bigintData = bigintData;
        this.timestampData = timestampData;
        this.integerData = integerData;
        this.booleanData = booleanData;
        this.doubleData = doubleData;
        this.varcharData = varcharData;
        this.hyperLogLogData = hyperLogLogData;
        this.jsonData = jsonData;
        this.dateData = dateData;
        this.dataReference = theOnlyNonNull(bigintData, timestampData, integerData, booleanData, doubleData, varcharData, hyperLogLogData, jsonData, dateData);
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public PrestoThriftBigint getBigintData()
    {
        return bigintData;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public PrestoThriftTimestamp getTimestampData()
    {
        return timestampData;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public PrestoThriftInteger getIntegerData()
    {
        return integerData;
    }

    @Nullable
    @ThriftField(value = 4, requiredness = OPTIONAL)
    public PrestoThriftBoolean getBooleanData()
    {
        return booleanData;
    }

    @Nullable
    @ThriftField(value = 5, requiredness = OPTIONAL)
    public PrestoThriftDouble getDoubleData()
    {
        return doubleData;
    }

    @Nullable
    @ThriftField(value = 6, requiredness = OPTIONAL)
    public PrestoThriftVarchar getVarcharData()
    {
        return varcharData;
    }

    @Nullable
    @ThriftField(value = 7, requiredness = OPTIONAL)
    public PrestoThriftHyperLogLog getHyperLogLogData()
    {
        return hyperLogLogData;
    }

    @Nullable
    @ThriftField(value = 8, requiredness = OPTIONAL)
    public PrestoThriftJson getJsonData()
    {
        return jsonData;
    }

    @Nullable
    @ThriftField(value = 9, requiredness = OPTIONAL)
    public PrestoThriftDate getDateData()
    {
        return dateData;
    }

    public static PrestoThriftColumnData bigintData(PrestoThriftBigint bigintData)
    {
        return new PrestoThriftColumnData(bigintData, null, null, null, null, null, null, null, null);
    }

    public static PrestoThriftColumnData timestampData(PrestoThriftTimestamp timestampData)
    {
        return new PrestoThriftColumnData(null, timestampData, null, null, null, null, null, null, null);
    }

    public static PrestoThriftColumnData integerData(PrestoThriftInteger integerData)
    {
        return new PrestoThriftColumnData(null, null, integerData, null, null, null, null, null, null);
    }

    public static PrestoThriftColumnData booleanData(PrestoThriftBoolean booleanData)
    {
        return new PrestoThriftColumnData(null, null, null, booleanData, null, null, null, null, null);
    }

    public static PrestoThriftColumnData doubleData(PrestoThriftDouble doubleData)
    {
        return new PrestoThriftColumnData(null, null, null, null, doubleData, null, null, null, null);
    }

    public static PrestoThriftColumnData varcharData(PrestoThriftVarchar varcharData)
    {
        return new PrestoThriftColumnData(null, null, null, null, null, varcharData, null, null, null);
    }

    public static PrestoThriftColumnData hyperLogLogData(PrestoThriftHyperLogLog hyperLogLogData)
    {
        return new PrestoThriftColumnData(null, null, null, null, null, null, hyperLogLogData, null, null);
    }

    public static PrestoThriftColumnData jsonData(PrestoThriftJson jsonData)
    {
        return new PrestoThriftColumnData(null, null, null, null, null, null, null, jsonData, null);
    }

    public static PrestoThriftColumnData dateData(PrestoThriftDate dateData)
    {
        return new PrestoThriftColumnData(null, null, null, null, null, null, null, null, dateData);
    }

    public Block toBlock(Type desiredType)
    {
        return dataReference.toBlock(desiredType);
    }

    public int numberOfRecords()
    {
        return dataReference.numberOfRecords();
    }

    public static ColumnBuilder builder(Type columnType, int initialCapacity)
    {
        switch (columnType.getTypeSignature().getBase()) {
            case BIGINT:
                return PrestoThriftBigint.builder(initialCapacity);
            case TIMESTAMP:
                return PrestoThriftTimestamp.builder(initialCapacity);
            case INTEGER:
                return PrestoThriftInteger.builder(initialCapacity);
            case BOOLEAN:
                return PrestoThriftBoolean.builder(initialCapacity);
            case DOUBLE:
                return PrestoThriftDouble.builder(initialCapacity);
            case VARCHAR:
                return PrestoThriftVarchar.builder(initialCapacity);
            case HYPER_LOG_LOG:
                return PrestoThriftHyperLogLog.builder(initialCapacity);
            case JSON:
                return PrestoThriftJson.builder(initialCapacity);
            case DATE:
                return PrestoThriftDate.builder(initialCapacity);
            default:
                throw new IllegalArgumentException("Unsupported writer type: " + columnType);
        }
    }

    private static PrestoThriftColumnType theOnlyNonNull(PrestoThriftColumnType... types)
    {
        PrestoThriftColumnType result = null;
        for (PrestoThriftColumnType type : types) {
            if (type != null) {
                checkState(result == null, "more than one type is present");
                result = type;
            }
        }
        checkState(result != null, "no types are present");
        return result;
    }
}
