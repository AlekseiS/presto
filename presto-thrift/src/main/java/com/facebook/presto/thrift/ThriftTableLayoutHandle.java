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
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.thrift.interfaces.client.ThriftTableLayout;
import com.facebook.presto.thrift.interfaces.client.ThriftTableLayoutResult;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.thrift.util.ByteUtils.summarize;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ThriftTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final byte[] layoutId;
    private final TupleDomain<ColumnHandle> predicate;

    @JsonCreator
    public ThriftTableLayoutHandle(
            @JsonProperty("layoutId") byte[] layoutId,
            @JsonProperty("predicate") TupleDomain<ColumnHandle> predicate)
    {
        this.layoutId = requireNonNull(layoutId, "layoutId is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public byte[] getLayoutId()
    {
        return layoutId;
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

        ThriftTableLayoutHandle other = (ThriftTableLayoutHandle) o;
        return Arrays.equals(layoutId, other.layoutId)
                && Objects.equals(predicate, other.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(layoutId, predicate);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("layoutId", summarize(layoutId))
                .add("predicate", predicate)
                .toString();
    }

    public static ConnectorTableLayout thriftTableLayoutToConnectorTableLayout(
            ThriftTableLayout thriftLayout,
            Map<String, ColumnHandle> allColumns)
    {
        TupleDomain<ColumnHandle> predicate = ThriftColumnHandle.thriftTupleDomainToTupleDomain(thriftLayout.getPredicate(), allColumns);
        return new ConnectorTableLayout(
                new ThriftTableLayoutHandle(thriftLayout.getLayoutId(), predicate),
                Optional.empty(),
                predicate,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of()
        );
    }

    public static ConnectorTableLayoutResult thriftLayoutResultToTableLayoutResult(
            ThriftTableLayoutResult result,
            Map<String, ColumnHandle> allColumns)
    {
        return new ConnectorTableLayoutResult(
                thriftTableLayoutToConnectorTableLayout(result.getLayout(), allColumns),
                ThriftColumnHandle.thriftTupleDomainToTupleDomain(result.getUnenforcedPredicate(), allColumns));
    }
}
