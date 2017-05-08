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

import java.util.List;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftSplit
{
    private final byte[] splitId;
    private final List<PrestoThriftHostAddress> hosts;

    @ThriftConstructor
    public PrestoThriftSplit(byte[] splitId, List<PrestoThriftHostAddress> hosts)
    {
        this.splitId = requireNonNull(splitId, "splitId is null");
        this.hosts = requireNonNull(hosts, "hosts is null");
    }

    @ThriftField(1)
    public byte[] getSplitId()
    {
        return splitId;
    }

    @ThriftField(2)
    public List<PrestoThriftHostAddress> getHosts()
    {
        return hosts;
    }
}
