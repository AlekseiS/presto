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
package com.facebook.presto.thrift.interfaces.readwrite;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.thrift.interfaces.client.ThriftColumnData;
import com.facebook.presto.thrift.interfaces.client.ThriftRowsBatch;
import com.facebook.presto.thrift.interfaces.readers.ColumnReaders;
import com.facebook.presto.thrift.interfaces.writers.ColumnWriter;
import com.facebook.presto.thrift.interfaces.writers.ColumnWriters;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestColumnReaderWriter
{
    private static final double NULL_FRACTION = 0.1;
    private static final int MAX_VARCHAR_GENERATED_LENGTH = 32;
    private static final char[] SYMBOLS;
    private static final long MIN_GENERATED_TIMESTAMP;
    private static final long MAX_GENERATED_TIMESTAMP;
    private final AtomicLong seedGenerator = new AtomicLong(762103512L);

    static {
        char[] symbols = new char[2 * 26 + 10];
        int next = 0;
        for (char ch = 'A'; ch <= 'Z'; ch++) {
            symbols[next++] = ch;
        }
        for (char ch = 'a'; ch <= 'z'; ch++) {
            symbols[next++] = ch;
        }
        for (char ch = '0'; ch <= '9'; ch++) {
            symbols[next++] = ch;
        }
        SYMBOLS = symbols;

        Calendar calendar = Calendar.getInstance();

        calendar.set(2000, Calendar.JANUARY, 1);
        MIN_GENERATED_TIMESTAMP = calendar.getTimeInMillis();

        calendar.set(2020, Calendar.DECEMBER, 31);
        MAX_GENERATED_TIMESTAMP = calendar.getTimeInMillis();
    }

    @Test(invocationCount = 10)
    public void testReadWrite()
            throws Exception
    {
        List<ColumnDefinition> columns = ImmutableList.of(
                new BigintColumn("c1"),
                new IntegerColumn("c2"),
                new TinyintColumn("c3"),
                new BooleanColumn("c4"),
                new DoubleColumn("c5"),
                new VarcharColumn("c6", createUnboundedVarcharType()),
                new VarcharColumn("c7", createVarcharType(MAX_VARCHAR_GENERATED_LENGTH / 2)),
                new TimestampColumn("c8")
        );

        Random random = new Random(seedGenerator.incrementAndGet());
        int records = random.nextInt(10000) + 10000;

        // generate columns data
        List<Block> inputBlocks = new ArrayList<>(columns.size());
        for (ColumnDefinition column : columns) {
            inputBlocks.add(generateColumn(column, random, records));
        }

        // convert column data to thrift ("write step")
        List<ThriftColumnData> columnsData = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            columnsData.addAll(writeColumnAsThrift(columns.get(i), inputBlocks.get(i)));
        }
        ThriftRowsBatch batch = new ThriftRowsBatch(columnsData, records, null);

        // convert thrift data to page/blocks ("read step")
        Page page = ColumnReaders.convertToPage(
                batch,
                columns.stream().map(ColumnDefinition::getName).collect(toImmutableList()),
                columns.stream().map(ColumnDefinition::getType).collect(toImmutableList()));

        // compare the result with original input
        assertNotNull(page);
        assertEquals(page.getChannelCount(), columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Block actual = page.getBlock(i);
            Block expected = inputBlocks.get(i);
            assertBlock(actual, expected, columns.get(i));
        }
    }

    private static List<ThriftColumnData> writeColumnAsThrift(ColumnDefinition column, Block block)
    {
        ColumnWriter bigintWriter = ColumnWriters.create(column.getName(), column.getType());
        for (int i = 0; i < block.getPositionCount(); i++) {
            bigintWriter.append(block, i, column.getType());
        }
        return bigintWriter.getResult();
    }

    private static Block generateColumn(ColumnDefinition column, Random random, int records)
    {
        BlockBuilder builder = column.getType().createBlockBuilder(new BlockBuilderStatus(), records);
        for (int i = 0; i < records; i++) {
            if (random.nextDouble() < NULL_FRACTION) {
                builder.appendNull();
            }
            else {
                column.writeNextRandomValue(random, builder);
            }
        }
        return builder.build();
    }

    private static void assertBlock(Block actual, Block expected, ColumnDefinition columnDefinition)
    {
        assertEquals(actual.getPositionCount(), expected.getPositionCount());
        int positions = actual.getPositionCount();
        for (int i = 0; i < positions; i++) {
            Object actualValue = columnDefinition.extractValue(actual, i);
            Object expectedValue = columnDefinition.extractValue(expected, i);
            assertEquals(actualValue, expectedValue);
        }
    }

    private static String nextString(Random random)
    {
        int size = random.nextInt(MAX_VARCHAR_GENERATED_LENGTH);
        char[] result = new char[size];
        for (int i = 0; i < size; i++) {
            result[i] = SYMBOLS[random.nextInt(SYMBOLS.length)];
        }
        return new String(result);
    }

    private static long nextTimestamp(Random random)
    {
        return MIN_GENERATED_TIMESTAMP + (long) (random.nextDouble() * (MAX_GENERATED_TIMESTAMP - MIN_GENERATED_TIMESTAMP));
    }

    private abstract static class ColumnDefinition
    {
        private final String name;
        private final Type type;

        public ColumnDefinition(String name, Type type)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        public String getName()
        {
            return name;
        }

        public Type getType()
        {
            return type;
        }

        abstract Object extractValue(Block block, int position);

        abstract void writeNextRandomValue(Random random, BlockBuilder builder);
    }

    private static final class BigintColumn
            extends ColumnDefinition
    {
        public BigintColumn(String name)
        {
            super(name, BIGINT);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return BIGINT.getLong(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            BIGINT.writeLong(builder, random.nextLong());
        }
    }

    private static final class TimestampColumn
            extends ColumnDefinition
    {
        public TimestampColumn(String name)
        {
            super(name, TIMESTAMP);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return TIMESTAMP.getLong(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            TIMESTAMP.writeLong(builder, nextTimestamp(random));
        }
    }

    private static final class IntegerColumn
            extends ColumnDefinition
    {
        public IntegerColumn(String name)
        {
            super(name, INTEGER);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return INTEGER.getLong(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            INTEGER.writeLong(builder, random.nextInt());
        }
    }

    private static final class TinyintColumn
            extends ColumnDefinition
    {
        public TinyintColumn(String name)
        {
            super(name, TINYINT);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return TINYINT.getLong(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            TINYINT.writeLong(builder, random.nextInt(256) + (int) Byte.MIN_VALUE);
        }
    }

    private static final class BooleanColumn
            extends ColumnDefinition
    {
        public BooleanColumn(String name)
        {
            super(name, BOOLEAN);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return BOOLEAN.getBoolean(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            BOOLEAN.writeBoolean(builder, random.nextBoolean());
        }
    }

    private static final class DoubleColumn
            extends ColumnDefinition
    {
        public DoubleColumn(String name)
        {
            super(name, DOUBLE);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return DOUBLE.getDouble(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            DOUBLE.writeDouble(builder, random.nextDouble());
        }
    }

    private static final class VarcharColumn
            extends ColumnDefinition
    {
        private final VarcharType varcharType;

        public VarcharColumn(String name, VarcharType varcharType)
        {
            super(name, varcharType);
            this.varcharType = requireNonNull(varcharType, "varcharType is null");
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return varcharType.getSlice(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            varcharType.writeString(builder, nextString(random));
        }
    }
}
