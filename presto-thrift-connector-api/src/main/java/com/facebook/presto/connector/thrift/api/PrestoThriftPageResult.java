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

import java.util.Objects;

import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftPageResult
{
    private final PrestoThriftPage page;
    private final PrestoThriftId nextToken;

    @ThriftConstructor
    public PrestoThriftPageResult(PrestoThriftPage page, @Nullable PrestoThriftId nextToken)
    {
        this.page = requireNonNull(page, "page is null");
        this.nextToken = nextToken;
    }

    @ThriftField(1)
    public PrestoThriftPage getPage()
    {
        return page;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public PrestoThriftId getNextToken()
    {
        return nextToken;
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
        PrestoThriftPageResult other = (PrestoThriftPageResult) obj;
        return Objects.equals(this.page, other.page) &&
                Objects.equals(this.nextToken, other.nextToken);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(page, nextToken);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("page", page)
                .add("nextToken", nextToken)
                .toString();
    }
}
