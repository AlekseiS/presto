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

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.connector.thrift.api.utils.NameValidationUtils.checkValidNames;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftStruct
public final class PrestoThriftTupleDomain
{
    private final Map<String, PrestoThriftDomain> domains;

    @ThriftConstructor
    public PrestoThriftTupleDomain(@Nullable Map<String, PrestoThriftDomain> domains)
    {
        if (domains != null) {
            checkValidNames(domains.keySet());
        }
        this.domains = domains;
    }

    /**
     * Return a map of column names to constraints.
     */
    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public Map<String, PrestoThriftDomain> getDomains()
    {
        return domains;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(domains);
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
        PrestoThriftTupleDomain other = (PrestoThriftTupleDomain) obj;
        return Objects.equals(this.domains, other.domains);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnsWithConstraints", domains != null ? domains.keySet() : ImmutableSet.of())
                .toString();
    }
}
