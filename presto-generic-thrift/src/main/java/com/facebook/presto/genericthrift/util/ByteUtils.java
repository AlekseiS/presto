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
package com.facebook.presto.genericthrift.util;

import com.google.common.io.BaseEncoding;

public final class ByteUtils
{
    private static final int PREFIX_SUFFIX_BYTES = 8;
    private static final String FILLER = "...";
    private static final int MAX_DISPLAY_CHARACTERS = PREFIX_SUFFIX_BYTES * 4 + FILLER.length();

    private ByteUtils()
    {
    }

    public static String summarize(byte[] value)
    {
        if (value == null) {
            return null;
        }
        if (value.length * 2 <= MAX_DISPLAY_CHARACTERS) {
            return BaseEncoding.base16().encode(value);
        }
        return BaseEncoding.base16().encode(value, 0, PREFIX_SUFFIX_BYTES)
                + FILLER
                + BaseEncoding.base16().encode(value, value.length - PREFIX_SUFFIX_BYTES, PREFIX_SUFFIX_BYTES);
    }
}
