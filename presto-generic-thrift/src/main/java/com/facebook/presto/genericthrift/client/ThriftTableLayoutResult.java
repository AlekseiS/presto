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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import java.util.Map;

import static com.facebook.presto.genericthrift.client.ThriftTableLayout.toConnectorTableLayout;
import static com.facebook.presto.genericthrift.client.ThriftTupleDomain.toTupleDomain;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class ThriftTableLayoutResult
{
    private final ThriftTableLayout layout;
    private final ThriftTupleDomain unenforcedPredicate;

    @ThriftConstructor
    public ThriftTableLayoutResult(ThriftTableLayout layout, ThriftTupleDomain unenforcedPredicate)
    {
        this.layout = requireNonNull(layout, "layout is null");
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
    }

    @ThriftField(1)
    public ThriftTableLayout getLayout()
    {
        return layout;
    }

    @ThriftField(2)
    public ThriftTupleDomain getUnenforcedPredicate()
    {
        return unenforcedPredicate;
    }

    public static ConnectorTableLayoutResult toConnectorTableLayoutResult(
            ThriftTableLayoutResult result,
            Map<String, ColumnHandle> allColumns)
    {
        return new ConnectorTableLayoutResult(
                toConnectorTableLayout(result.getLayout(), allColumns),
                toTupleDomain(result.getUnenforcedPredicate(), allColumns));
    }
}
