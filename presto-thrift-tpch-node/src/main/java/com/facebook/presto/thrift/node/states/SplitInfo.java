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
package com.facebook.presto.thrift.node.states;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class SplitInfo
{
    private final String schemaName;
    private final String tableName;
    private final int startPartNumber;
    private final int endPartNumber;
    private final int totalParts;
    private final List<String> columnNames;

    @JsonCreator
    public SplitInfo(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("startPartNumber") int startPartNumber,
            @JsonProperty("endPartNumber") int endPartNumber,
            @JsonProperty("totalParts") int totalParts,
            @JsonProperty("columnNames") List<String> columnNames)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.startPartNumber = startPartNumber;
        this.endPartNumber = endPartNumber;
        this.totalParts = totalParts;
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public int getStartPartNumber()
    {
        return startPartNumber;
    }

    @JsonProperty
    public int getEndPartNumber()
    {
        return endPartNumber;
    }

    @JsonProperty
    public int getTotalParts()
    {
        return totalParts;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }
}
