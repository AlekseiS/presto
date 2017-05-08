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

import com.facebook.presto.connector.thrift.api.PrestoThriftColumnData;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.BigintType;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestLongColumnWriter
{
    private static final String COLUMN = "column1";

    @Test
    public void testAlternatingResizing()
            throws Exception
    {
        testAlternating(2);
    }

    @Test
    public void testAlternatingOversized()
            throws Exception
    {
        testAlternating(30);
    }

    private void testAlternating(int capacity)
    {
        LongColumnWriter writer = new LongColumnWriter(COLUMN, capacity);
        Block source = longBlock(1, null, 2, null, 3, null, 4, null, 5, null, 6, null, 7, null);
        for (int i = 0; i < source.getPositionCount(); i++) {
            writer.append(source, i, BigintType.BIGINT);
        }
        List<PrestoThriftColumnData> result = writer.getResult();
        assertEquals(result.size(), 1);
        PrestoThriftColumnData column = result.get(0);
        assertEquals(column.getColumnName(), COLUMN);
        assertTrue(column.isOnlyNullsAndLongs());
        assertEquals(column.getNulls(), new boolean[] {false, true, false, true, false, true, false, true, false, true, false, true, false, true});
        assertEquals(column.getLongs(), new long[] {1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0});
    }

    @Test
    public void testAllNulls()
            throws Exception
    {
        LongColumnWriter writer = new LongColumnWriter(COLUMN, 10);
        Block source = longBlock(null, null, null, null, null);
        for (int i = 0; i < source.getPositionCount(); i++) {
            writer.append(source, i, BigintType.BIGINT);
        }
        List<PrestoThriftColumnData> result = writer.getResult();
        assertEquals(result.size(), 1);
        PrestoThriftColumnData column = result.get(0);
        assertEquals(column.getColumnName(), COLUMN);
        assertTrue(column.isOnlyNullsAndLongs());
        assertEquals(column.getNulls(), new boolean[] {true, true, true, true, true});
        assertNull(column.getLongs());
    }

    @Test
    public void testAllNonNull()
            throws Exception
    {
        LongColumnWriter writer = new LongColumnWriter(COLUMN, 10);
        Block source = longBlock(1, 2, 3, 4, 5);
        for (int i = 0; i < source.getPositionCount(); i++) {
            writer.append(source, i, BigintType.BIGINT);
        }
        List<PrestoThriftColumnData> result = writer.getResult();
        assertEquals(result.size(), 1);
        PrestoThriftColumnData column = result.get(0);
        assertEquals(column.getColumnName(), COLUMN);
        assertTrue(column.isOnlyNullsAndLongs());
        assertNull(column.getNulls());
        assertEquals(column.getLongs(), new long[] {1, 2, 3, 4, 5});
    }

    @Test
    public void testEmpty()
            throws Exception
    {
        LongColumnWriter writer = new LongColumnWriter(COLUMN, 10);
        List<PrestoThriftColumnData> result = writer.getResult();
        assertEquals(result.size(), 1);
        PrestoThriftColumnData column = result.get(0);
        assertEquals(column.getColumnName(), COLUMN);
        assertTrue(column.isOnlyNullsAndLongs());
        assertNull(column.getNulls());
        assertNull(column.getLongs());
    }

    private static Block longBlock(Integer... values)
    {
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(new BlockBuilderStatus(), values.length);
        for (Integer value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeLong(value).closeEntry();
            }
        }
        return blockBuilder.build();
    }
}
