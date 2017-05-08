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
package com.facebook.presto.connector.thrift.api.builders;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;

final class BuilderUtils
{
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private BuilderUtils()
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
        checkArgument(expectedLength < booleans.length, "expectedLength is greater than the size of an array");
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
        checkArgument(expectedLength < bytes.length, "expectedLength is greater than the size of an array");
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
        checkArgument(expectedLength < ints.length, "expectedLength is greater than the size of an array");
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
        checkArgument(expectedLength < longs.length, "expectedLength is greater than the size of an array");
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
        checkArgument(expectedLength < doubles.length, "expectedLength is greater than the size of an array");
        return Arrays.copyOf(doubles, expectedLength);
    }

    public static int doubleCapacityChecked(int position)
    {
        if (position >= MAX_ARRAY_SIZE) {
            throw new IllegalStateException("Cannot allocate an array larger than " + MAX_ARRAY_SIZE + " bytes");
        }
        else {
            return position >= MAX_ARRAY_SIZE / 2 ? MAX_ARRAY_SIZE : 2 * max(position, 1);
        }
    }
}
