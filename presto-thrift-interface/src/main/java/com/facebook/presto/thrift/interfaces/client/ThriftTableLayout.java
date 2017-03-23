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
package com.facebook.presto.thrift.interfaces.client;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class ThriftTableLayout
{
    private final byte[] layoutId;
    private final ThriftTupleDomain predicate;

    @ThriftConstructor
    public ThriftTableLayout(byte[] layoutId, ThriftTupleDomain predicate)
    {
        this.layoutId = requireNonNull(layoutId, "layoutId is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @ThriftField(1)
    public byte[] getLayoutId()
    {
        return layoutId;
    }

    @ThriftField(2)
    public ThriftTupleDomain getPredicate()
    {
        return predicate;
    }
}
