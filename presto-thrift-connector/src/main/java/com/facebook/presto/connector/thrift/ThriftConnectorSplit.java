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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.connector.thrift.util.ByteUtils.summarize;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ThriftConnectorSplit
        implements ConnectorSplit
{
    private final byte[] splitId;
    private final List<HostAddress> addresses;

    @JsonCreator
    public ThriftConnectorSplit(
            @JsonProperty("splitId") byte[] splitId,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.splitId = requireNonNull(splitId, "splitId is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @JsonProperty
    public byte[] getSplitId()
    {
        return splitId;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("splitId", summarize(splitId))
                .add("addresses", addresses)
                .toString();
    }
}
