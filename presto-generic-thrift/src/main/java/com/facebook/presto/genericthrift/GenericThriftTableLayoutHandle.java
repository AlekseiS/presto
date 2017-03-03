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
package com.facebook.presto.genericthrift;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GenericThriftTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final SchemaTableName schemaTableName;
    private final Optional<List<ColumnHandle>> outputColumns;
    private final TupleDomain<ColumnHandle> predicate;

    @JsonCreator
    public GenericThriftTableLayoutHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("outputColumns") Optional<List<ColumnHandle>> outputColumns,
            @JsonProperty("predicate") TupleDomain<ColumnHandle> predicate)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getOutputColumns()
    {
        return outputColumns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPredicate()
    {
        return predicate;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GenericThriftTableLayoutHandle other = (GenericThriftTableLayoutHandle) o;
        return Objects.equals(schemaTableName, other.schemaTableName)
                && Objects.equals(outputColumns, other.outputColumns)
                && Objects.equals(predicate, other.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, outputColumns, predicate);
    }
}
