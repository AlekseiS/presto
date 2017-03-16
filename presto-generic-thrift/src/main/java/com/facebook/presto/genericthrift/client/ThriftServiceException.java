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

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public final class ThriftServiceException
        extends RuntimeException
{
    private final int code;
    private final String message;
    private final boolean retryPossible;

    @ThriftConstructor
    public ThriftServiceException(int code, String message, boolean retryPossible)
    {
        this.code = code;
        this.message = message;
        this.retryPossible = retryPossible;
    }

    @ThriftField(1)
    public int getCode()
    {
        return code;
    }

    @Override
    @ThriftField(2)
    public String getMessage()
    {
        return message;
    }

    @ThriftField(3)
    public boolean isRetryPossible()
    {
        return retryPossible;
    }
}
