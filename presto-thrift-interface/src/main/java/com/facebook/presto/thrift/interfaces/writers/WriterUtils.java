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
package com.facebook.presto.thrift.interfaces.writers;

import java.util.Arrays;

public final class WriterUtils
{
    private WriterUtils()
    {
    }

    public static boolean[] trim(boolean[] booleans, boolean hasValues, int expectedLength)
    {
        if (!hasValues) {
            return null;
        }
        if (booleans.length == expectedLength) {
            return booleans;
        }
        return Arrays.copyOf(booleans, expectedLength);
    }

    public static byte[] trim(byte[] bytes, boolean hasValues, int expectedLength)
    {
        if (!hasValues) {
            return null;
        }
        if (bytes.length == expectedLength) {
            return bytes;
        }
        return Arrays.copyOf(bytes, expectedLength);
    }

    public static int[] trim(int[] ints, boolean hasValues, int expectedLength)
    {
        if (!hasValues) {
            return null;
        }
        if (ints.length == expectedLength) {
            return ints;
        }
        return Arrays.copyOf(ints, expectedLength);
    }

    public static long[] trim(long[] longs, boolean hasValues, int expectedLength)
    {
        if (!hasValues) {
            return null;
        }
        if (longs.length == expectedLength) {
            return longs;
        }
        return Arrays.copyOf(longs, expectedLength);
    }

    public static double[] trim(double[] doubles, boolean hasValues, int expectedLength)
    {
        if (!hasValues) {
            return null;
        }
        if (doubles.length == expectedLength) {
            return doubles;
        }
        return Arrays.copyOf(doubles, expectedLength);
    }
}
