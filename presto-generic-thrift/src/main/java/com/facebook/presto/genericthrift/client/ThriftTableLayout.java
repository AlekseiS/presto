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

import com.facebook.presto.genericthrift.GenericThriftTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.genericthrift.client.ThriftTupleDomain.toTupleDomain;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThriftStruct
public final class ThriftTableLayout
{
    private final List<String> outputColumns;
    private final ThriftTupleDomain predicate;

    @ThriftConstructor
    public ThriftTableLayout(
            @ThriftField(name = "outputColumns") @Nullable List<String> outputColumns,
            @ThriftField(name = "predicate") ThriftTupleDomain predicate)
    {
        this.outputColumns = outputColumns == null ? null : ImmutableList.copyOf(outputColumns);
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public List<String> getOutputColumns()
    {
        return outputColumns;
    }

    @ThriftField(2)
    public ThriftTupleDomain getPredicate()
    {
        return predicate;
    }

    public static ConnectorTableLayout toConnectorTableLayout(ThriftTableLayout thriftLayout, SchemaTableName schemaTableName, Map<String, ColumnHandle> allColumns)
    {
        Optional<List<ColumnHandle>> columns = Optional.ofNullable(thriftLayout.getOutputColumns())
                .map(outputColumns -> outputColumns
                        .stream()
                        .map(columnName -> requireNonNull(allColumns.get(columnName), "Column handle is not present"))
                        .collect(toList()));
        TupleDomain<ColumnHandle> predicate = toTupleDomain(thriftLayout.getPredicate(), allColumns);
        return new ConnectorTableLayout(
                new GenericThriftTableLayoutHandle(schemaTableName, columns, predicate),
                columns,
                predicate,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of()
        );
    }
}
