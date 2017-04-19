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
package com.facebook.presto.thrift.node;

import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.tpch.TpchMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchTable;

import java.util.List;

import static com.facebook.presto.tpch.TpchMetadata.getPrestoType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;

final class TpchServerUtils
{
    private TpchServerUtils()
    {
    }

    public static int estimateRecords(long maxBytes, List<Type> types)
    {
        if (types.isEmpty()) {
            // no data will be returned, only row count
            return Integer.MAX_VALUE;
        }
        int bytesPerRow = 0;
        for (Type type : types) {
            if (type instanceof FixedWidthType) {
                bytesPerRow += ((FixedWidthType) type).getFixedSize();
            }
            else if (type instanceof VarcharType) {
                VarcharType varchar = (VarcharType) type;
                if (varchar.isUnbounded()) {
                    // random estimate
                    bytesPerRow += 32;
                }
                else {
                    bytesPerRow += varchar.getLengthSafe();
                }
            }
            else {
                throw new IllegalArgumentException("Cannot compute estimates for an unknown type: " + type);
            }
        }
        return toIntExact(maxBytes / bytesPerRow);
    }

    public static List<Integer> computeRemap(List<String> startSchema, List<String> endSchema)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (String columnName : endSchema) {
            int index = startSchema.indexOf(columnName);
            checkArgument(index != -1, "Column name in end that is not in the start: %s", columnName);
            builder.add(index);
        }
        return builder.build();
    }

    public static List<String> allColumns(String tableName)
    {
        return TpchTable.getTable(tableName).getColumns().stream().map(TpchColumn::getSimplifiedColumnName).collect(toList());
    }

    public static List<Type> types(String tableName, List<String> columnNames)
    {
        TpchTable<?> table = TpchTable.getTable(tableName);
        return columnNames.stream().map(name -> getPrestoType(table.getColumn(name).getType())).collect(toList());
    }

    public static double schemaNameToScaleFactor(String schemaName)
    {
        switch (schemaName) {
            case "tiny":
                return 0.01;
            case "sf1":
                return 1.0;
        }
        throw new IllegalArgumentException("Schema is not setup: " + schemaName);
    }

    public static String getTypeString(TpchColumnType tpchType)
    {
        return TpchMetadata.getPrestoType(tpchType).getTypeSignature().toString();
    }
}
