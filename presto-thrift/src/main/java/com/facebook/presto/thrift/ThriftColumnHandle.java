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
package com.facebook.presto.thrift;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.thrift.interfaces.client.ThriftDomain;
import com.facebook.presto.thrift.interfaces.client.ThriftTupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.thrift.interfaces.client.ThriftDomain.fromDomain;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public final class ThriftColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final String comment;
    private final boolean hidden;

    @JsonCreator
    public ThriftColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("comment") @Nullable String comment,
            @JsonProperty("hidden") boolean hidden)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.comment = comment;
        this.hidden = hidden;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @Nullable
    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, comment, hidden);
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
        ThriftColumnHandle other = (ThriftColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.comment, other.comment) &&
                this.hidden == other.hidden;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("comment", comment)
                .add("hidden", hidden)
                .toString();
    }

    public static Set<String> columnNames(Set<ColumnHandle> columns)
    {
        return columns.stream()
                .map(ThriftColumnHandle.class::cast)
                .map(ThriftColumnHandle::getColumnName)
                .collect(toImmutableSet());
    }

    public static List<String> columnNames(List<ColumnHandle> columns)
    {
        return columns.stream()
                .map(ThriftColumnHandle.class::cast)
                .map(ThriftColumnHandle::getColumnName)
                .collect(toImmutableList());
    }

    public static ThriftTupleDomain tupleDomainToThriftTupleDomain(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (!tupleDomain.getDomains().isPresent()) {
            return new ThriftTupleDomain(null);
        }
        Map<String, ThriftDomain> thriftDomains = tupleDomain.getDomains().get()
                .entrySet()
                .stream()
                .collect(toImmutableMap(
                        kv -> ((ThriftColumnHandle) kv.getKey()).getColumnName(),
                        kv -> fromDomain(kv.getValue())));
        return new ThriftTupleDomain(thriftDomains);
    }
}
