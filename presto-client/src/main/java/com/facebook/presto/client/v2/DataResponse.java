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
package com.facebook.presto.client.v2;

import java.net.URI;

public class DataResponse<T>
{
    private final T value;
    private final long byteSize;
    private final URI nextUri;

    public DataResponse(T value, long byteSize, URI nextUri)
    {
        this.value = value;
        this.byteSize = byteSize;
        this.nextUri = nextUri;
    }

    public T getValue()
    {
        return value;
    }

    public long getByteSize()
    {
        return byteSize;
    }

    public URI getNextUri()
    {
        return nextUri;
    }
}
