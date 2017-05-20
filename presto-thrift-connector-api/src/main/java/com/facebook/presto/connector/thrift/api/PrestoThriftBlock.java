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

import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBigint;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBoolean;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftColumnData;
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

import java.util.Objects;

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
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

@ThriftStruct
public final class PrestoThriftBlock
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
    private final PrestoThriftColumnData dataReference;

    @ThriftConstructor
    public PrestoThriftBlock(
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

    public Block toBlock(Type desiredType)
    {
        return dataReference.toBlock(desiredType);
    }

    public int numberOfRecords()
    {
        return dataReference.numberOfRecords();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftBlock other = (PrestoThriftBlock) obj;
        // remaining fields are guaranteed to be null by the constructor
        return Objects.equals(this.dataReference, other.dataReference);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bigintData, timestampData, integerData, booleanData, doubleData, varcharData, hyperLogLogData, jsonData, dateData);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("data", dataReference)
                .toString();
    }

    public static PrestoThriftBlock bigintData(PrestoThriftBigint bigintData)
    {
        return new PrestoThriftBlock(bigintData, null, null, null, null, null, null, null, null);
    }

    public static PrestoThriftBlock timestampData(PrestoThriftTimestamp timestampData)
    {
        return new PrestoThriftBlock(null, timestampData, null, null, null, null, null, null, null);
    }

    public static PrestoThriftBlock integerData(PrestoThriftInteger integerData)
    {
        return new PrestoThriftBlock(null, null, integerData, null, null, null, null, null, null);
    }

    public static PrestoThriftBlock booleanData(PrestoThriftBoolean booleanData)
    {
        return new PrestoThriftBlock(null, null, null, booleanData, null, null, null, null, null);
    }

    public static PrestoThriftBlock doubleData(PrestoThriftDouble doubleData)
    {
        return new PrestoThriftBlock(null, null, null, null, doubleData, null, null, null, null);
    }

    public static PrestoThriftBlock varcharData(PrestoThriftVarchar varcharData)
    {
        return new PrestoThriftBlock(null, null, null, null, null, varcharData, null, null, null);
    }

    public static PrestoThriftBlock hyperLogLogData(PrestoThriftHyperLogLog hyperLogLogData)
    {
        return new PrestoThriftBlock(null, null, null, null, null, null, hyperLogLogData, null, null);
    }

    public static PrestoThriftBlock jsonData(PrestoThriftJson jsonData)
    {
        return new PrestoThriftBlock(null, null, null, null, null, null, null, jsonData, null);
    }

    public static PrestoThriftBlock dateData(PrestoThriftDate dateData)
    {
        return new PrestoThriftBlock(null, null, null, null, null, null, null, null, dateData);
    }

    public static PrestoThriftBlock fromSingleValueBlock(Block block, Type type)
    {
        checkArgument(block.getPositionCount() == 1, "block must have exactly one value");
        switch (type.getTypeSignature().getBase()) {
            case BIGINT:
                return PrestoThriftBigint.fromSingleValueBlock(block);
            case TIMESTAMP:
                return PrestoThriftTimestamp.fromSingleValueBlock(block);
            case INTEGER:
                return PrestoThriftInteger.fromSingleValueBlock(block);
            case BOOLEAN:
                return PrestoThriftBoolean.fromSingleValueBlock(block);
            case DOUBLE:
                return PrestoThriftDouble.fromSingleValueBlock(block);
            case VARCHAR:
                return PrestoThriftVarchar.fromSingleValueBlock(block, type);
            case HYPER_LOG_LOG:
                return PrestoThriftHyperLogLog.fromSingleValueBlock(block);
            case JSON:
                return PrestoThriftJson.fromSingleValueBlock(block, type);
            case DATE:
                return PrestoThriftDate.fromSingleValueBlock(block);
            default:
                throw new IllegalArgumentException("Unsupported block type: " + type);
        }
    }

    private static PrestoThriftColumnData theOnlyNonNull(PrestoThriftColumnData... columnsData)
    {
        PrestoThriftColumnData result = null;
        for (PrestoThriftColumnData data : columnsData) {
            if (data != null) {
                checkArgument(result == null, "more than one type is present");
                result = data;
            }
        }
        checkArgument(result != null, "no types are present");
        return result;
    }
}
