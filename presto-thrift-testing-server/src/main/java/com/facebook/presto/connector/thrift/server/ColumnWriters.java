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
package com.facebook.presto.connector.thrift.server;

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBigint;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBoolean;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftDate;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftDouble;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftHyperLogLog;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftInteger;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftJson;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftTimestamp;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftVarchar;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.function.BiFunction;

import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.bigintData;
import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.booleanData;
import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.dateData;
import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.doubleData;
import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.hyperLogLogData;
import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.integerData;
import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.jsonData;
import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.timestampData;
import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.varcharData;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.google.common.base.Preconditions.checkState;

public final class ColumnWriters
{
    private ColumnWriters()
    {
    }

    public static PrestoThriftBlock toThriftBlock(Block block, Type type)
    {
        switch (type.getTypeSignature().getBase()) {
            case INTEGER:
                return toIntBasedThriftBlock(block, type, (nulls, ints) -> integerData(new PrestoThriftInteger(nulls, ints)));
            case BIGINT:
                return bigintData(PrestoThriftBigint.fromBlock(block));
            case DOUBLE:
                return toDoubleThriftBlock(block, type);
            case VARCHAR:
                return toSliceBasedThriftBlock(block, type, (nulls, sizes, bytes) -> varcharData(new PrestoThriftVarchar(nulls, sizes, bytes)));
            case BOOLEAN:
                return toBooleanThriftBlock(block, type);
            case DATE:
                return toIntBasedThriftBlock(block, type, (nulls, ints) -> dateData(new PrestoThriftDate(nulls, ints)));
            case TIMESTAMP:
                return toLongBasedThriftBlock(block, type, (nulls, longs) -> timestampData(new PrestoThriftTimestamp(nulls, longs)));
            case JSON:
                return toSliceBasedThriftBlock(block, type, (nulls, sizes, bytes) -> jsonData(new PrestoThriftJson(nulls, sizes, bytes)));
            case HYPER_LOG_LOG:
                return toSliceBasedThriftBlock(block, type, (nulls, sizes, bytes) -> hyperLogLogData(new PrestoThriftHyperLogLog(nulls, sizes, bytes)));
            default:
                throw new IllegalArgumentException("Unsupported block type: " + type);
        }
    }

    private static PrestoThriftBlock toLongBasedThriftBlock(Block block, Type type, BiFunction<boolean[], long[], PrestoThriftBlock> result)
    {
        int positions = block.getPositionCount();
        if (positions == 0) {
            return result.apply(null, null);
        }
        boolean[] nulls = null;
        long[] longs = null;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                if (longs == null) {
                    longs = new long[positions];
                }
                longs[position] = type.getLong(block, position);
            }
        }
        return result.apply(nulls, longs);
    }

    private static PrestoThriftBlock toIntBasedThriftBlock(Block block, Type type, BiFunction<boolean[], int[], PrestoThriftBlock> result)
    {
        int positions = block.getPositionCount();
        if (positions == 0) {
            return result.apply(null, null);
        }
        boolean[] nulls = null;
        int[] ints = null;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                if (ints == null) {
                    ints = new int[positions];
                }
                ints[position] = (int) type.getLong(block, position);
            }
        }
        return result.apply(nulls, ints);
    }

    private static PrestoThriftBlock toBooleanThriftBlock(Block block, Type type)
    {
        int positions = block.getPositionCount();
        if (positions == 0) {
            return booleanData(new PrestoThriftBoolean(null, null));
        }
        boolean[] nulls = null;
        boolean[] booleans = null;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                if (booleans == null) {
                    booleans = new boolean[positions];
                }
                booleans[position] = type.getBoolean(block, position);
            }
        }
        return booleanData(new PrestoThriftBoolean(nulls, booleans));
    }

    private static PrestoThriftBlock toDoubleThriftBlock(Block block, Type type)
    {
        int positions = block.getPositionCount();
        if (positions == 0) {
            return booleanData(new PrestoThriftBoolean(null, null));
        }
        boolean[] nulls = null;
        double[] doubles = null;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                if (doubles == null) {
                    doubles = new double[positions];
                }
                doubles[position] = type.getDouble(block, position);
            }
        }
        return doubleData(new PrestoThriftDouble(nulls, doubles));
    }

    private static PrestoThriftBlock toSliceBasedThriftBlock(Block block, Type type, CreateSliceFunction create)
    {
        int positions = block.getPositionCount();
        if (positions == 0) {
            return create.apply(null, null, null);
        }
        boolean[] nulls = null;
        int[] sizes = null;
        byte[] bytes = null;
        int bytesIndex = 0;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                Slice value = type.getSlice(block, position);
                if (sizes == null) {
                    sizes = new int[positions];
                    int totalBytes = totalSliceBytes(block);
                    if (totalBytes > 0) {
                        bytes = new byte[totalBytes];
                    }
                }
                int length = value.length();
                sizes[position] = length;
                if (length > 0) {
                    checkState(bytes != null);
                    value.getBytes(0, bytes, bytesIndex, length);
                    bytesIndex += length;
                }
            }
        }
        checkState(bytes == null || bytesIndex == bytes.length);
        return create.apply(nulls, sizes, bytes);
    }

    private static int totalSliceBytes(Block block)
    {
        int totalBytes = 0;
        int positions = block.getPositionCount();
        for (int position = 0; position < positions; position++) {
            totalBytes += block.getSliceLength(position);
        }
        return totalBytes;
    }

    private interface CreateSliceFunction
    {
        PrestoThriftBlock apply(boolean[] nulls, int[] sizes, byte[] bytes);
    }
}
