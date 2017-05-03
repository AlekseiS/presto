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
package com.facebook.presto.connector.thrift.writers;

import org.testng.annotations.Test;

import static com.facebook.presto.connector.thrift.writers.WriterUtils.doubleCapacityChecked;
import static com.facebook.presto.connector.thrift.writers.WriterUtils.trim;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class WriterUtilsTest
{
    @Test
    public void testTrimBooleans()
            throws Exception
    {
        boolean[] initial = new boolean[] {true, false, true, false, true};
        assertNull(trim(initial, false, 10));
        assertSame(trim(initial, true, 5), initial);
        assertEquals(trim(initial, true, 4), new boolean[] {true, false, true, false});
        assertEquals(trim(initial, true, 1), new boolean[] {true});
        assertEquals(trim(initial, true, 0), new boolean[] {});
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTrimBooleansInvalid()
            throws Exception
    {
        trim(new boolean[] {true, false}, true, 10);
    }

    @Test
    public void testDoubleCapacityChecked()
            throws Exception
    {
        assertEquals(doubleCapacityChecked(1), 2);
        assertEquals(doubleCapacityChecked(123), 246);
        assertEquals(doubleCapacityChecked(Integer.MAX_VALUE / 2 - 128), (Integer.MAX_VALUE / 2 - 128) * 2);
        assertEquals(doubleCapacityChecked(Integer.MAX_VALUE - 100), Integer.MAX_VALUE - 8);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDoubleCapacityCheckedInvalid()
            throws Exception
    {
        doubleCapacityChecked(Integer.MAX_VALUE - 1);
    }
}
