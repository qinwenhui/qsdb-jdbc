package cn.qinwh.qsdb.jdbc.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * {@code qsdb-rows} 编码器单测。
 *
 * <p><b>核心是 {@link #specExampleFromDocs()}——内核 {@code docs/rows-format.md}
 * §3.3 的 85 字节示例逐字节锁定</b>。内核侧有同名测试（{@code rows.rs} 的
 * {@code spec_example_from_docs}）逐字节守护同一段字节，两侧对死即协议对齐，
 * 无需联网即可验证实现正确性。
 */
class RowsEncoderTest {

    private static RowsEncoder.Column col(String name, QsdbType t) {
        return new RowsEncoder.Column(name, t, 0);
    }

    /** 一个块 = 若干行（每行一个 Object[]）。显式辅助避免 asList 的 varargs 展开歧义。 */
    private static List<Object[]> chunk(Object[]... rows) {
        return Arrays.asList(rows);
    }

    /** payload 的块列表。 */
    @SafeVarargs
    private static List<List<Object[]>> chunks(List<Object[]>... cs) {
        return Arrays.asList(cs);
    }

    // ---- 规范示例（85 字节）----

    /**
     * 表：id BIGINT / name STRING / dt DATE。
     * 块 1 = 2 行（第 1 行整行 NULL），块 2 = 1 行。
     * 期望字节逐字取自内核 docs/rows-format.md §3.3。
     */
    @Test
    void specExampleFromDocs() {
        List<RowsEncoder.Column> cols = Arrays.asList(
                col("id", QsdbType.BIGINT),
                col("name", QsdbType.VARCHAR),
                col("dt", QsdbType.DATE));

        // 块 1：row0 = (1, "alice", 19000)，row1 = 全 NULL
        List<Object[]> chunk1 = new ArrayList<Object[]>();
        chunk1.add(new Object[] {1L, "alice", 19000L});
        chunk1.add(new Object[] {null, null, null});
        // 块 2：row = (3, "中文", 19001)
        List<Object[]> chunk2 = new ArrayList<Object[]>();
        chunk2.add(new Object[] {3L, "中文", 19001L});

        byte[] got = RowsEncoder.encode(cols, Arrays.asList(chunk1, chunk2));

        byte[] want = new byte[] {
                // magic "QSDBROWS"
                0x51, 0x53, 0x44, 0x42, 0x52, 0x4f, 0x57, 0x53,
                // version = 1（u16 LE）
                0x01, 0x00,
                // flags = 0（写侧无 schema）
                0x00,
                // n_cols = 3（u16 LE）
                0x03, 0x00,
                // --- 块 1 ---
                // row_count = 2（u32 LE）
                0x02, 0x00, 0x00, 0x00,
                // chunk_flags = HAS_NULLS
                0x01,
                // 3 列位图（每列 1 字节；第 1 行置位 → 0b10）
                0x02, 0x02, 0x02,
                // row0：id = 1（i64 LE）
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // row0：name = "alice"（u32 len + 字节）
                0x05, 0x00, 0x00, 0x00, 0x61, 0x6c, 0x69, 0x63, 0x65,
                // row0：dt = 19000 = 0x4A38（i32 LE）
                0x38, 0x4a, 0x00, 0x00,
                // row1 全 NULL 占位：id（8 字节 0）
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // row1 占位：name（len = 0）
                0x00, 0x00, 0x00, 0x00,
                // row1 占位：dt（4 字节 0）
                0x00, 0x00, 0x00, 0x00,
                // --- 块 2 ---
                // row_count = 1
                0x01, 0x00, 0x00, 0x00,
                // chunk_flags = 0（无 NULL → 位图整段不出现）
                0x00,
                // id = 3
                0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // name = "中文"（6 字节 UTF-8）
                0x06, 0x00, 0x00, 0x00, (byte) 0xe4, (byte) 0xb8, (byte) 0xad,
                (byte) 0xe6, (byte) 0x96, (byte) 0x87,
                // dt = 19001 = 0x4A39
                0x39, 0x4a, 0x00, 0x00};

        assertEquals(85, want.length, "规范示例应为 85 字节");
        assertEquals(85, got.length, "编码器输出长度不符（got " + got.length + "）");
        assertArrayEquals(want, got, "编码结果与内核 docs/rows-format.md §3.3 逐字节不符");
    }

    // ---- 头 ----

    @Test
    void headerShape() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowsEncoder.writeHeader(out, 7);
        assertArrayEquals(new byte[] {
                'Q', 'S', 'D', 'B', 'R', 'O', 'W', 'S',
                0x01, 0x00,   // version 1
                0x00,         // flags 0
                0x07, 0x00},  // n_cols 7
                out.toByteArray());
    }

    // ---- 类型编码 ----

    private static byte[] encodeOne(QsdbType type, Object value) {
        return RowsEncoder.encode(
                Arrays.asList(new RowsEncoder.Column("c", type, 2)),
                chunks(chunk(new Object[] {value})));
    }

    private static byte[] valuesOf(byte[] payload, int width) {
        // 头 13 字节 + chunk(row_count 4 + flags 1) = 18；无 NULL 时值紧随其后
        return Arrays.copyOfRange(payload, 18, 18 + width);
    }

    @Test
    void fixedWidthTypes() {
        // i8 / i16 / i32 / i64
        assertArrayEquals(new byte[] {(byte) 0xFF},
                valuesOf(encodeOne(QsdbType.TINYINT, -1L), 1));
        assertArrayEquals(new byte[] {(byte) 0xD2, (byte) 0x04},
                valuesOf(encodeOne(QsdbType.SMALLINT, 1234L), 2));
        assertArrayEquals(new byte[] {(byte) 0xD2, 0x04, 0x00, 0x00},
                valuesOf(encodeOne(QsdbType.INT, 1234L), 4));
        assertArrayEquals(new byte[] {(byte) 0xD2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                valuesOf(encodeOne(QsdbType.BIGINT, 1234L), 8));
        // 负 i64（补码 LE）
        assertArrayEquals(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
                valuesOf(encodeOne(QsdbType.BIGINT, -1L), 8));
        // f64：1.5 = 0x3FF8000000000000 → LE
        assertArrayEquals(new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xF8, 0x3F},
                valuesOf(encodeOne(QsdbType.DOUBLE, 1.5d), 8));
        // f32：1.5 = 0x3FC00000 → LE
        assertArrayEquals(new byte[] {0x00, 0x00, (byte) 0xC0, 0x3F},
                valuesOf(encodeOne(QsdbType.FLOAT, 1.5d), 4));
        // boolean
        assertArrayEquals(new byte[] {1}, valuesOf(encodeOne(QsdbType.BOOLEAN, Boolean.TRUE), 1));
        assertArrayEquals(new byte[] {0}, valuesOf(encodeOne(QsdbType.BOOLEAN, Boolean.FALSE), 1));
        // date = epoch days
        assertArrayEquals(new byte[] {0x38, 0x4A, 0x00, 0x00},
                valuesOf(encodeOne(QsdbType.DATE, 19000L), 4));
        // time = nanos since midnight（08:30:00 = 30.6e12 = 0x1BD49E215000 → LE）
        assertArrayEquals(new byte[] {0x00, 0x50, 0x21, (byte) 0x9E,
                        (byte) 0xD4, 0x1B, 0x00, 0x00},
                valuesOf(encodeOne(QsdbType.TIME, 30_600_000_000_000L), 8));
        // timestamp = epoch nanos（1.7e18 = 0x17979CFE362A0000 → LE）
        assertArrayEquals(new byte[] {0x00, 0x00, 0x2A, 0x36,
                        (byte) 0xFE, (byte) 0x9C, (byte) 0x97, 0x17},
                valuesOf(encodeOne(QsdbType.TIMESTAMP, 1_700_000_000_000_000_000L), 8));
    }

    @Test
    void variableLengthTypes() {
        // Utf8：len + UTF-8
        byte[] s = encodeOne(QsdbType.VARCHAR, "中文");
        assertArrayEquals(new byte[] {0x06, 0x00, 0x00, 0x00,
                        (byte) 0xe4, (byte) 0xb8, (byte) 0xad, (byte) 0xe6, (byte) 0x96, (byte) 0x87},
                Arrays.copyOfRange(s, 18, s.length));
        // Binary：len + 字节
        byte[] b = encodeOne(QsdbType.BINARY, new byte[] {(byte) 0xde, (byte) 0xad});
        assertArrayEquals(new byte[] {0x02, 0x00, 0x00, 0x00, (byte) 0xde, (byte) 0xad},
                Arrays.copyOfRange(b, 18, b.length));
    }

    @Test
    void decimal128LittleEndianUnscaled() {
        // DECIMAL(_, 2)：12.34 → 未缩放 1234 = 0x04D2 → LE 首字节 0xD2
        byte[] d = encodeOne(QsdbType.DECIMAL, new BigDecimal("12.34"));
        byte[] v = valuesOf(d, 16);
        assertEquals(0xD2, v[0] & 0xFF);
        assertEquals(0x04, v[1] & 0xFF);
        for (int i = 2; i < 16; i++) {
            assertEquals(0, v[i], "高位应为 0（正数符号扩展）");
        }
        // 负数：-1.00 → 未缩放 -100 → 16 字节全 0xFF 的补码 0xFF9C
        byte[] neg = valuesOf(encodeOne(QsdbType.DECIMAL, new BigDecimal("-1.00")), 16);
        assertEquals((byte) 0x9C, neg[0]);
        assertEquals((byte) 0xFF, neg[1]);
        for (int i = 2; i < 16; i++) {
            assertEquals((byte) 0xFF, neg[i], "高位应为 0xFF（负数符号扩展）");
        }
    }

    @Test
    void decimalScaleMismatchIsRefusedNotRounded() {
        // 12.345 放进 scale=2 的列 → 显式拒绝（不静默四舍五入改数据）
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> encodeOne(QsdbType.DECIMAL, new BigDecimal("12.345")));
        assertTrue(e.getMessage().contains("round"), e.getMessage());
        // 值超出 Decimal128 精度
        assertThrows(IllegalArgumentException.class,
                () -> encodeOne(QsdbType.DECIMAL, new BigDecimal("1e40")));
    }

    // ---- NULL 与位图 ----

    @Test
    void nullsUseBitmapAndKeepPlaceholders() {
        // 一列 I64，3 行：非空 / NULL / 非空 → 位图 1 字节 = 0b010 = 2
        byte[] payload = RowsEncoder.encode(
                Arrays.asList(col("c", QsdbType.BIGINT)),
                chunks(chunk(new Object[] {7L}, new Object[] {null}, new Object[] {9L})));
        // 头 13 + row_count 4 + flags 1 = 18 → 位图 1 字节（18）
        assertEquals(1, payload[17], "chunk_flags 应为 HAS_NULLS");
        assertEquals(2, payload[18], "第 1 行应为 NULL 位");
        // 值紧随位图：7 / 占位 0 / 9
        int base = 19;
        assertEquals(7L, readI64(payload, base));
        assertEquals(0L, readI64(payload, base + 8), "NULL 行必须占位（全 0）");
        assertEquals(9L, readI64(payload, base + 16));
        assertEquals(19 + 24, payload.length);
    }

    @Test
    void bitmapCoversAllColumnsWhenAnyColumnHasNull() {
        // 两列，仅第 0 列第 1 行为 NULL → 两列都要出位图，第 1 列为全 0
        byte[] payload = RowsEncoder.encode(
                Arrays.asList(col("a", QsdbType.BIGINT), col("b", QsdbType.BIGINT)),
                chunks(chunk(new Object[] {1L, 2L}, new Object[] {null, 3L})));
        assertEquals(1, payload[17], "chunk_flags 应为 HAS_NULLS");
        assertEquals(2, payload[18], "列 0 位图：第 1 行置位");
        assertEquals(0, payload[19], "列 1 无 NULL → 全 0 位图");
        // row-major：row0 = (1, 2), row1 = (0 占位, 3)
        int base = 20;
        assertEquals(1L, readI64(payload, base));
        assertEquals(2L, readI64(payload, base + 8));
        assertEquals(0L, readI64(payload, base + 16));
        assertEquals(3L, readI64(payload, base + 24));
    }

    @Test
    void nullStringKeepsZeroLength() {
        byte[] payload = RowsEncoder.encode(
                Arrays.asList(col("s", QsdbType.VARCHAR)),
                chunks(chunk(new Object[] {null})));
        // 头 13 + row_count 4 + flags 1 + 位图 1 = 19，值是 len=0 的 4 字节
        assertEquals(1, payload[17], "chunk_flags 应为 HAS_NULLS");
        assertEquals(1, payload[18], "唯一一行是 NULL → 位图 bit0 置位");
        assertEquals(23, payload.length, "NULL 变长值写 len=0（4 字节占位）");
    }

    // ---- 多块 ----

    @Test
    void multipleChunksAreConcatenated() {
        List<RowsEncoder.Column> cols = Arrays.asList(col("c", QsdbType.BIGINT));
        byte[] one = RowsEncoder.encode(cols, chunks(chunk(new Object[] {1L})));
        byte[] two = RowsEncoder.encode(cols,
                chunks(chunk(new Object[] {1L}), chunk(new Object[] {2L})));
        // 第二个 payload = 头 + 两个块
        byte[] secondChunk = Arrays.copyOfRange(two, one.length, two.length);
        byte[] rebuilt = new byte[one.length + secondChunk.length];
        System.arraycopy(one, 0, rebuilt, 0, one.length);
        System.arraycopy(secondChunk, 0, rebuilt, one.length, secondChunk.length);
        assertArrayEquals(rebuilt, two, "多块 = 头 + 依次拼接的块");
    }

    @Test
    void emptyChunkIsLegalShape() {
        List<RowsEncoder.Column> cols = Arrays.asList(col("c", QsdbType.BIGINT));
        byte[] payload = RowsEncoder.encode(cols, Arrays.asList(new ArrayList<Object[]>()));
        // 头 13 + row_count(0) 4 + flags 1
        assertEquals(18, payload.length);
        assertEquals(0, payload[13]);
        assertEquals(0, payload[17], "零行块不应有位图");
    }

    // ---- 类型标签 ----

    @Test
    void typeTagsMatchSpec() {
        assertEquals(1, RowsEncoder.typeTag(QsdbType.BOOLEAN));
        assertEquals(2, RowsEncoder.typeTag(QsdbType.INT));
        assertEquals(3, RowsEncoder.typeTag(QsdbType.BIGINT));
        assertEquals(4, RowsEncoder.typeTag(QsdbType.DOUBLE));
        assertEquals(5, RowsEncoder.typeTag(QsdbType.VARCHAR));
        assertEquals(6, RowsEncoder.typeTag(QsdbType.DATE));
        assertEquals(7, RowsEncoder.typeTag(QsdbType.TIMESTAMP));
        assertEquals(8, RowsEncoder.typeTag(QsdbType.TINYINT));
        assertEquals(9, RowsEncoder.typeTag(QsdbType.SMALLINT));
        assertEquals(10, RowsEncoder.typeTag(QsdbType.FLOAT));
        assertEquals(11, RowsEncoder.typeTag(QsdbType.BINARY));
        assertEquals(12, RowsEncoder.typeTag(QsdbType.DECIMAL));
        assertEquals(13, RowsEncoder.typeTag(QsdbType.TIME));
        assertEquals(14, RowsEncoder.typeTag(QsdbType.INTERVAL));
    }

    @Test
    void magicIsAscii() {
        assertArrayEquals("QSDBROWS".getBytes(StandardCharsets.US_ASCII), RowsEncoder.MAGIC);
    }

    private static long readI64(byte[] b, int off) {
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (b[off + i] & 0xFFL);
        }
        return v;
    }
}
