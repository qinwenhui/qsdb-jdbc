package cn.qinwh.qsdb.jdbc.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import cn.qinwh.qsdb.jdbc.QsdbException;
import cn.qinwh.qsdb.jdbc.transport.ColMeta;
import cn.qinwh.qsdb.jdbc.transport.Value;

/**
 * {@code qsdb-rows} 行式零依赖批量格式的解码器（**读侧**，{@code flags=1}
 * 带列描述）。格式权威源：内核 {@code docs/rows-format.md}。
 *
 * <p>与 JSON 读路径的关系：产出<b>同样的</b> {@link ColMeta}（类型串用 Arrow
 * 方言，交给 TypeStrings 归一）与 {@link Value} 判别联合，因此下游
 * {@code QsdbResultSet} 的类型转换与 {@code wasNull} 语义<b>完全复用</b>——
 * 换格式不改语义。两者的一致性由差分集成测试（同一查询 json vs rows 逐值
 * 比对）守护。
 *
 * <p>个别类型的取值形态与 JSON 路径有意不同（更快且更精确）：
 * <ul>
 *   <li>BINARY → {@code byte[]}（JSON 是 hex 字符串，需再解码）</li>
 *   <li>DATE/TIME/TIMESTAMP(naive) → {@code long}（天数/自午夜纳秒/自 epoch
 *       纳秒）——{@code QsdbResultSet} 的 LONG 分支处理，语义与 JSON 的
 *       文本形态一致</li>
 *   <li>DECIMAL → 精确十进制串（与 JSON 修复后同形，避免精度歧义）</li>
 *   <li>TIMESTAMP(带时区) → RFC3339 串（与 JSON 同形）</li>
 *   <li>INTERVAL → 显式拒绝（驱动侧本就不支持其取值；调用方可回退 json）</li>
 * </ul>
 *
 * <p>internal API——不在驱动公开面上。
 */
public final class RowsDecoder {

    /** 解码结果：列元数据 + 行集。 */
    public static final class Decoded {
        public final List<ColMeta> columns;
        public final List<List<Value>> rows;

        Decoded(List<ColMeta> columns, List<List<Value>> rows) {
            this.columns = columns;
            this.rows = rows;
        }
    }

    /** 每列的解码规格（类型标签 + 该类型需要的 meta）。 */
    private static final class ColSpec {
        final int tag;
        final int decimalScale;
        final boolean tzAware;

        ColSpec(int tag, int decimalScale, boolean tzAware) {
            this.tag = tag;
            this.decimalScale = decimalScale;
            this.tzAware = tzAware;
        }
    }

    private RowsDecoder() {
    }

    /**
     * 响应体是否为 {@code qsdb-rows} 载荷（嗅探 8 字节 magic）。
     *
     * <p>用途：**非 SELECT 语句即使点名 {@code format=rows}，内核仍回 JSON
     * 回执**（没有行可编码），故调用方须先判别再选解码路径。定长 magic 正是
     * 为这种判别设计的。
     */
    public static boolean looksLikeRows(byte[] body) {
        if (body == null || body.length < RowsEncoder.MAGIC.length) {
            return false;
        }
        for (int i = 0; i < RowsEncoder.MAGIC.length; i++) {
            if (body[i] != RowsEncoder.MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * 解码完整 payload（头 + 任意多个块）。
     *
     * @param payload {@code qsdb-rows} 字节流
     * @throws QsdbException HY000：格式非法/类型不支持（消息带偏移量，可诊断）
     */
    public static Decoded decode(byte[] payload) throws QsdbException {
        Cursor c = new Cursor(payload);
        if (payload.length < 13 || !c.matchesMagic(RowsEncoder.MAGIC)) {
            throw c.err("bad magic (not a qsdb-rows payload)");
        }
        c.skip(8);
        int version = c.u16();
        if (version != RowsEncoder.VERSION) {
            throw c.err("unsupported rows format version " + version);
        }
        int flags = c.u8();
        int nCols = c.u16();
        boolean hasSchema = (flags & 1) != 0;
        if (!hasSchema) {
            throw c.err("read-side payload must carry column descriptions (flags bit0 = 1)");
        }

        // 列描述
        List<ColMeta> cols = new ArrayList<ColMeta>(nCols);
        List<ColSpec> specs = new ArrayList<ColSpec>(nCols);
        for (int i = 0; i < nCols; i++) {
            int nameLen = c.u16();
            String name = c.utf8(nameLen);
            int tag = c.u8();
            c.u8(); // col_flags（bit0 = nullable）——取值不依赖它，"是否 NULL 由位图定"
            int metaLen = c.u16();
            String tz = null;
            int scale = 0;
            String typeString = typeStringFor(tag, null);
            if (metaLen > 0) {
                byte[] meta = c.take(metaLen);
                if (tag == RowsEncoder.T_TIMESTAMP && metaLen >= 2) {
                    // meta = tz_len u16 + tz
                    int tzLen = (meta[0] & 0xFF) | ((meta[1] & 0xFF) << 8);
                    if (tzLen > 0 && 2 + tzLen <= meta.length) {
                        tz = new String(meta, 2, tzLen, StandardCharsets.UTF_8);
                    }
                    typeString = typeStringFor(tag, tz);
                } else if (tag == RowsEncoder.T_DECIMAL128 && metaLen >= 2) {
                    // meta = precision u8 + scale i8
                    scale = meta[1]; // 有符号
                    typeString = "Decimal128(" + (meta[0] & 0xFF) + ", " + scale + ")";
                }
            }
            cols.add(new ColMeta(name, typeString));
            specs.add(new ColSpec(tag, scale, tz != null));
        }

        // 块 × N
        List<List<Value>> rows = new ArrayList<List<Value>>();
        while (!c.eof()) {
            long rowCount = c.u32();
            if (rowCount > Integer.MAX_VALUE) {
                throw c.err("chunk row_count too large: " + rowCount);
            }
            int nRows = (int) rowCount;
            int chunkFlags = c.u8();
            boolean chunkHasNulls = (chunkFlags & 1) != 0;
            int bitmapBytes = chunkHasNulls ? (nRows + 7) / 8 : 0;
            // 位图：n_cols 段，列序拼接（LSB first）
            byte[][] bitmaps = new byte[nCols][];
            if (chunkHasNulls) {
                for (int i = 0; i < nCols; i++) {
                    bitmaps[i] = c.take(bitmapBytes);
                }
            }
            // 值：行主序
            for (int r = 0; r < nRows; r++) {
                List<Value> row = new ArrayList<Value>(nCols);
                for (int i = 0; i < nCols; i++) {
                    Value v = readValue(c, specs.get(i));
                    if (chunkHasNulls && bitmaps[i] != null
                            && (bitmaps[i][r >> 3] & (1 << (r & 7))) != 0) {
                        v = Value.NULL; // 位图为准（占位值已读取以推进游标）
                    }
                    row.add(v);
                }
                rows.add(java.util.Collections.unmodifiableList(row));
            }
        }
        return new Decoded(cols, rows);
    }

    /**
     * 读一个值（**NULL 行也要读**——格式要求占位照写，位图另行判定）。
     */
    private static Value readValue(Cursor c, ColSpec spec) throws QsdbException {
        int tag = spec.tag;
        int decimalScale = spec.decimalScale;
        switch (tag) {
            case RowsEncoder.T_BOOLEAN:
                return Value.of(c.u8() != 0);
            case RowsEncoder.T_INT8:
                return Value.of((long) (byte) c.u8());
            case RowsEncoder.T_INT16:
                return Value.of((long) (short) c.u16());
            case RowsEncoder.T_INT32:
                return Value.of((long) (int) c.u32());
            case RowsEncoder.T_INT64:
                return Value.of(c.u64());
            case RowsEncoder.T_FLOAT32:
                return Value.of((double) Float.intBitsToFloat((int) c.u32()));
            case RowsEncoder.T_FLOAT64:
                return Value.of(Double.longBitsToDouble(c.u64()));
            case RowsEncoder.T_UTF8:
                return Value.of(c.utf8Checked((int) c.u32()));
            case RowsEncoder.T_BINARY:
                return Value.of(c.take((int) c.u32()));
            case RowsEncoder.T_DATE:
                return Value.of((long) (int) c.u32()); // epoch 天数
            case RowsEncoder.T_TIME:
                return Value.of(c.u64()); // 自午夜纳秒
            case RowsEncoder.T_TIMESTAMP:
                long tsNanos = c.u64();
                // 带时区 → RFC3339 串（绝对时刻，与 JSON 路径同形）；
                // naive → 自 epoch 纳秒，下游按"该纳秒值即 UTC 墙钟"还原
                // （与 JSON 通道把 naive 显示串按本地墙钟解析的语义一致）
                return spec.tzAware ? Value.of(formatRfc3339(tsNanos)) : Value.of(tsNanos);
            case RowsEncoder.T_DECIMAL128:
                return Value.of(readDecimal128(c, decimalScale));
            default:
                throw c.err("unsupported column type tag " + tag
                        + " in rows payload (retry with readFormat=json)");
        }
    }

    /**
     * 16 字节 i128 小端 → 精确十进制串（按列标度还原小数点）。
     * 输出形态与 JSON 通道修复后一致，故下游 {@code getBigDecimal} 无需分支。
     */
    private static String readDecimal128(Cursor c, int scale) throws QsdbException {
        byte[] le = c.take(16);
        byte[] be = new byte[16]; // 小端 → 大端二进制补码
        for (int i = 0; i < 16; i++) {
            be[i] = le[15 - i];
        }
        BigInteger unscaled = new BigInteger(be);
        return new BigDecimal(unscaled, scale).toPlainString();
    }

    /** 自 epoch 纳秒 → RFC3339（与内核 JSON 通道的带时区形态一致）。 */
    private static String formatRfc3339(long epochNanos) {
        long secs = Math.floorDiv(epochNanos, 1_000_000_000L);
        long nanos = Math.floorMod(epochNanos, 1_000_000_000L);
        return java.time.format.DateTimeFormatter.ISO_INSTANT
                .format(java.time.Instant.ofEpochSecond(secs, nanos));
    }

    /** 类型标签 → Arrow 方言类型串（交给既有的 TypeStrings 归一，零新映射表）。 */
    static String typeStringFor(int tag, String tz) {
        switch (tag) {
            case RowsEncoder.T_BOOLEAN:
                return "Boolean";
            case RowsEncoder.T_INT8:
                return "Int8";
            case RowsEncoder.T_INT16:
                return "Int16";
            case RowsEncoder.T_INT32:
                return "Int32";
            case RowsEncoder.T_INT64:
                return "Int64";
            case RowsEncoder.T_FLOAT32:
                return "Float32";
            case RowsEncoder.T_FLOAT64:
                return "Float64";
            case RowsEncoder.T_UTF8:
                return "Utf8";
            case RowsEncoder.T_BINARY:
                return "Binary";
            case RowsEncoder.T_DATE:
                return "Date32";
            case RowsEncoder.T_TIME:
                return "Time64(Nanosecond)";
            case RowsEncoder.T_TIMESTAMP:
                return tz == null
                        ? "Timestamp(Nanosecond, None)"
                        : "Timestamp(Nanosecond, Some(\"" + tz + "\"))";
            case RowsEncoder.T_DECIMAL128:
                // 精度/标度须由 meta 提供；调用方按 meta 覆写（见 decode 的 DECIMAL 通道）
                return "Decimal128(38, 0)";
            default:
                return "Unknown(" + tag + ")";
        }
    }

    // ------------------------------------------------------------------
    // 游标
    // ------------------------------------------------------------------

    /** 顺序读取的小端游标（越界即报错，绝不静默截断）。 */
    static final class Cursor {
        private final byte[] b;
        private int pos;

        Cursor(byte[] b) {
            this.b = b;
        }

        boolean eof() {
            return pos >= b.length;
        }

        QsdbException err(String msg) {
            return new QsdbException(
                    "malformed qsdb-rows payload at offset " + pos + ": " + msg, "HY000");
        }

        boolean matchesMagic(byte[] magic) {
            if (pos + magic.length > b.length) {
                return false;
            }
            for (int i = 0; i < magic.length; i++) {
                if (b[pos + i] != magic[i]) {
                    return false;
                }
            }
            return true;
        }

        void skip(int n) {
            pos += n;
        }

        byte[] take(int n) throws QsdbException {
            if (n < 0 || pos + n > b.length) {
                throw err("truncated (need " + n + " bytes, have " + (b.length - pos) + ")");
            }
            byte[] out = new byte[n];
            System.arraycopy(b, pos, out, 0, n);
            pos += n;
            return out;
        }

        int u8() throws QsdbException {
            if (pos >= b.length) {
                throw err("truncated (u8)");
            }
            return b[pos++] & 0xFF;
        }

        int u16() throws QsdbException {
            int lo = u8();
            return lo | (u8() << 8);
        }

        long u32() throws QsdbException {
            return (long) u16() | ((long) u16() << 16);
        }

        long u64() throws QsdbException {
            return u32() | (u32() << 32);
        }

        String utf8(int n) throws QsdbException {
            return new String(take(n), StandardCharsets.UTF_8);
        }

        /** UTF-8 严格解码（非法字节序列报错，不做替换）。 */
        String utf8Checked(int n) throws QsdbException {
            byte[] raw = take(n);
            try {
                java.nio.charset.CharsetDecoder dec = StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                        .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
                return dec.decode(java.nio.ByteBuffer.wrap(raw)).toString();
            } catch (java.nio.charset.CharacterCodingException e) {
                throw err("invalid UTF-8 in string value");
            }
        }
    }

}
