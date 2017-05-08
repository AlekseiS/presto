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

import javax.annotation.Nullable;

import java.util.Arrays;

import static com.facebook.presto.connector.thrift.api.utils.ByteUtils.summarize;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftStruct
public final class PrestoThriftNullableToken
{
    private final byte[] token;

    @ThriftConstructor
    public PrestoThriftNullableToken(@Nullable byte[] token)
    {
        this.token = token;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public byte[] getToken()
    {
        return token;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(token);
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
        PrestoThriftNullableToken other = (PrestoThriftNullableToken) obj;
        return Arrays.equals(this.token, other.token);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("token", summarize(token))
                .toString();
    }
}
