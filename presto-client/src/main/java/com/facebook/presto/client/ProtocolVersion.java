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
package com.facebook.presto.client;

import static java.util.Objects.requireNonNull;

public enum ProtocolVersion
{
    V1, V2;

    public static ProtocolVersion fromString(String value)
    {
        requireNonNull(value, "value is null");
        switch (value) {
            case "v1":
                return V1;
            case "v2":
                return V2;
            default:
                throw new IllegalArgumentException("Unknown value: " + value);
        }
    }
}
