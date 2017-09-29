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
package com.facebook.presto.connector.thrift.server;

import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestListBasedRecordSet
{
    @Test
    public void testEmptyCursor()
            throws Exception
    {
        ListBasedRecordSet recordSet = new ListBasedRecordSet(ImmutableList.of(), ImmutableList.of(BIGINT, INTEGER));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, INTEGER));
        RecordCursor cursor = recordSet.cursor();
        assertFalse(cursor.advanceNextPosition());
    }

    @Test
    public void testCursor()
            throws Exception
    {
        ListBasedRecordSet recordSet = new ListBasedRecordSet(
                ImmutableList.of(
                        ImmutableList.of(1L, 10L),
                        ImmutableList.of(2L, 20L),
                        ImmutableList.of(3L, 30L)),
                ImmutableList.of(BIGINT, INTEGER));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, INTEGER));
        RecordCursor cursor = recordSet.cursor();
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getType(0), BIGINT);
        assertEquals(cursor.getType(1), INTEGER);
        assertThrows(IndexOutOfBoundsException.class, () -> cursor.getLong(2));
        assertEquals(cursor.getLong(0), 1L);
        assertEquals(cursor.getLong(1), 10L);
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 2L);
        assertEquals(cursor.getLong(1), 20L);
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 3L);
        assertEquals(cursor.getLong(1), 30L);
        assertFalse(cursor.advanceNextPosition());
        assertThrows(IndexOutOfBoundsException.class, () -> cursor.getLong(0));
    }
}