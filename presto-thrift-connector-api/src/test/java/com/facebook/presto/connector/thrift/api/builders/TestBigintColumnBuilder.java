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

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBigint;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestBigintColumnBuilder
{
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
        ColumnBuilder builder = PrestoThriftBigint.builder(capacity);
        Block source = longBlock(1, null, 2, null, 3, null, 4, null, 5, null, 6, null, 7, null);
        for (int i = 0; i < source.getPositionCount(); i++) {
            builder.append(source, i, BIGINT);
        }
        PrestoThriftBlock column = builder.build();
        assertNotNull(column.getBigintData());
        assertEquals(column.getBigintData().getNulls(),
                new boolean[] {false, true, false, true, false, true, false, true, false, true, false, true, false, true});
        assertEquals(column.getBigintData().getLongs(),
                new long[] {1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0});
    }

    @Test
    public void testAllNulls()
            throws Exception
    {
        ColumnBuilder builder = PrestoThriftBigint.builder(10);
        Block source = longBlock(null, null, null, null, null);
        for (int i = 0; i < source.getPositionCount(); i++) {
            builder.append(source, i, BIGINT);
        }
        PrestoThriftBlock column = builder.build();
        assertNotNull(column.getBigintData());
        assertEquals(column.getBigintData().getNulls(), new boolean[] {true, true, true, true, true});
        assertNull(column.getBigintData().getLongs());
    }

    @Test
    public void testAllNonNull()
            throws Exception
    {
        ColumnBuilder builder = PrestoThriftBigint.builder(10);
        Block source = longBlock(1, 2, 3, 4, 5);
        for (int i = 0; i < source.getPositionCount(); i++) {
            builder.append(source, i, BIGINT);
        }
        PrestoThriftBlock column = builder.build();
        assertNotNull(column.getBigintData());
        assertNull(column.getBigintData().getNulls());
        assertEquals(column.getBigintData().getLongs(), new long[] {1, 2, 3, 4, 5});
    }

    @Test
    public void testEmpty()
            throws Exception
    {
        ColumnBuilder builder = PrestoThriftBigint.builder(10);
        PrestoThriftBlock column = builder.build();
        assertNotNull(column.getBigintData());
        assertNull(column.getBigintData().getNulls());
        assertNull(column.getBigintData().getLongs());
    }

    @Test
    public void testEmptyZeroCapacity()
            throws Exception
    {
        ColumnBuilder builder = PrestoThriftBigint.builder(0);
        PrestoThriftBlock column = builder.build();
        assertNotNull(column.getBigintData());
        assertNull(column.getBigintData().getNulls());
        assertNull(column.getBigintData().getLongs());
    }

    @Test
    public void testNonEmptyZeroCapacity()
            throws Exception
    {
        ColumnBuilder builder = PrestoThriftBigint.builder(0);
        builder.append(longBlock(1), 0, BIGINT);
        PrestoThriftBlock column = builder.build();
        assertNotNull(column.getBigintData());
        assertNull(column.getBigintData().getNulls());
        assertEquals(column.getBigintData().getLongs(), new long[] {1});
    }

    private static Block longBlock(Integer... values)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), values.length);
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
