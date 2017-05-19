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

import java.util.Arrays;

import static com.facebook.presto.connector.thrift.api.utils.ByteUtils.summarize;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftId
{
    private final byte[] id;

    @ThriftConstructor
    public PrestoThriftId(byte[] id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @ThriftField(1)
    public byte[] getId()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(id);
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
        PrestoThriftId other = (PrestoThriftId) obj;
        return Arrays.equals(this.id, other.id);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", summarize(id))
                .toString();
    }
}
