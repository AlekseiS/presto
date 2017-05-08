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

import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftTableLayoutResult
{
    private final PrestoThriftTableLayout layout;
    private final PrestoThriftTupleDomain unenforcedPredicate;

    @ThriftConstructor
    public PrestoThriftTableLayoutResult(PrestoThriftTableLayout layout, PrestoThriftTupleDomain unenforcedPredicate)
    {
        this.layout = requireNonNull(layout, "layout is null");
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
    }

    @ThriftField(1)
    public PrestoThriftTableLayout getLayout()
    {
        return layout;
    }

    @ThriftField(2)
    public PrestoThriftTupleDomain getUnenforcedPredicate()
    {
        return unenforcedPredicate;
    }
}
