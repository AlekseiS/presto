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

import com.facebook.presto.genericthrift.GenericThriftColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.genericthrift.client.ThriftDomain.fromDomain;
import static com.facebook.presto.genericthrift.client.ThriftDomain.toDomain;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

@ThriftStruct
public final class ThriftTupleDomain
{
    private final Map<String, ThriftDomain> domains;

    @ThriftConstructor
    public ThriftTupleDomain(
            @Nullable @ThriftField(name = "domains") Map<String, ThriftDomain> domains)
    {
        this.domains = domains == null ? null : ImmutableMap.copyOf(domains);
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public Map<String, ThriftDomain> getDomains()
    {
        return domains;
    }

    public static ThriftTupleDomain fromTupleDomain(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (!tupleDomain.getDomains().isPresent()) {
            return new ThriftTupleDomain(null);
        }
        Map<String, ThriftDomain> thriftDomains = tupleDomain.getDomains().get()
                .entrySet()
                .stream()
                .collect(toMap(
                        kv -> ((GenericThriftColumnHandle) kv.getKey()).getColumnName(),
                        kv -> fromDomain(kv.getValue())));
        return new ThriftTupleDomain(thriftDomains);
    }

    public static TupleDomain<ColumnHandle> toTupleDomain(
            ThriftTupleDomain thriftTupleDomain,
            Map<String, ColumnHandle> allColumns)
    {
        if (thriftTupleDomain.getDomains() == null) {
            return TupleDomain.none();
        }
        Map<ColumnHandle, Domain> tupleDomains = new HashMap<>();
        for (Map.Entry<String, ThriftDomain> kv : thriftTupleDomain.getDomains().entrySet()) {
            GenericThriftColumnHandle handle = (GenericThriftColumnHandle) requireNonNull(allColumns.get(kv.getKey()),
                    "Column handle is not present");
            Domain domain = toDomain(kv.getValue(), handle.getColumnType());
            tupleDomains.put(handle, domain);
        }
        return TupleDomain.withColumnDomains(tupleDomains);
    }
}
