package cn.qinwh.qsdb.jdbc.internal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import cn.qinwh.qsdb.jdbc.QsdbException;
import cn.qinwh.qsdb.jdbc.transport.ColMeta;
import cn.qinwh.qsdb.jdbc.transport.Value;

/**
 * {@code qsdb-rows} 流式读取器（P3：{@code setFetchSize} 真流式的读侧引擎）。
 *
 * <p>与 {@link RowsDecoder}（一次性 byte[] 解码）同源：magic/version/头/块帧
 * 完全一致（内核 docs/rows-format.md，version 1）；差别只在消费方式——
 * 本类从 {@link InputStream} <b>逐块</b>解码，内存 O(一块)，与总行数脱钩。
 * 产出同样的 {@link ColMeta}/{@link Value} 模型，下游 ResultSet 语义完全
 * 复用；两路一致性由差分集成测试（逐值比对）守护。
 *
 * <p>纪律与 RowsDecoder 相同：坏字节一律显式报错（消息带偏移量），流中断
 * 报截断，绝不"能读多少算多少"。
 *
 * <p>internal API——不在驱动公开面上。非线程安全（单游标顺序读）。
 */
public final class RowsStreamReader {

    /** 每列的解码规格（与 RowsDecoder 的 ColSpec 同形；类型 meta 在头里）。 */
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

    private final StreamCursor c;
    private final List<ColMeta> columns;
    private final List<ColSpec> specs;
    private boolean done; // 干净流尾已到达

    /**
     * 从流读头并解析列描述（{@code flags bit0=1} 强制——读侧契约）。
     *
     * @param in 已确认 magic 的响应体流（调用方先用
     *           {@link RowsDecoder#looksLikeRows} 嗅探前 8 字节——非 SELECT
     *           语句内核回 JSON 回执，不是本类的输入）
     */
    public RowsStreamReader(InputStream in) throws QsdbException {
        this.c = new StreamCursor(in);
        int version = c.u16();
        if (version != RowsEncoder.VERSION) {
            throw c.err("unsupported rows format version " + version);
        }
        int flags = c.u8();
        int nCols = c.u16();
        if ((flags & 1) == 0) {
            throw c.err("read-side payload must carry column descriptions (flags bit0 = 1)");
        }
        this.columns = new ArrayList<ColMeta>(nCols);
        this.specs = new ArrayList<ColSpec>(nCols);
        for (int i = 0; i < nCols; i++) {
            int nameLen = c.u16();
            String name = c.utf8(nameLen);
            int tag = c.u8();
            c.u8(); // col_flags（bit0=nullable）——取值不依赖，"是否 NULL 由位图定"
            int metaLen = c.u16();
            String tz = null;
            int scale = 0;
            String typeString = RowsDecoder.typeStringFor(tag, null);
            if (metaLen > 0) {
                byte[] meta = c.take(metaLen);
                if (tag == RowsEncoder.T_TIMESTAMP && metaLen >= 2) {
                    int tzLen = (meta[0] & 0xFF) | ((meta[1] & 0xFF) << 8);
                    if (tzLen > 0 && 2 + tzLen <= meta.length) {
                        tz = new String(meta, 2, tzLen, StandardCharsets.UTF_8);
                    }
                    typeString = RowsDecoder.typeStringFor(tag, tz);
                } else if (tag == RowsEncoder.T_DECIMAL128 && metaLen >= 2) {
                    scale = meta[1]; // 有符号
                    typeString = "Decimal128(" + (meta[0] & 0xFF) + ", " + scale + ")";
                }
            }
            columns.add(new ColMeta(name, typeString));
            specs.add(new ColSpec(tag, scale, tz != null));
        }
    }

    /** 列元数据（流一开始就完整可读——头先于任何行数据）。 */
    public List<ColMeta> columns() {
        return columns;
    }

    /**
     * 解码下一块；**null = 干净流尾**（结果集结束）。
     *
     * @throws QsdbException HY000：格式非法/流中断（消息带偏移量）
     */
    public List<List<Value>> nextChunk() throws QsdbException {
        if (done) {
            return null;
        }
        long rowCount = c.u32OrEof();
        if (rowCount < 0) {
            done = true; // 流尾干净结束（帧边界处恰好 EOF）
            return null;
        }
        if (rowCount > Integer.MAX_VALUE) {
            throw c.err("chunk row_count too large: " + rowCount);
        }
        int nRows = (int) rowCount;
        int chunkFlags = c.u8();
        if ((chunkFlags & ~1) != 0) {
            throw c.err("unknown chunk flags: " + chunkFlags);
        }
        boolean chunkHasNulls = (chunkFlags & 1) != 0;
        int bitmapBytes = chunkHasNulls ? (nRows + 7) / 8 : 0;
        byte[][] bitmaps = new byte[specs.size()][];
        if (chunkHasNulls) {
            for (int i = 0; i < specs.size(); i++) {
                bitmaps[i] = c.take(bitmapBytes);
            }
        }
        List<List<Value>> rows = new ArrayList<List<Value>>(nRows);
        for (int r = 0; r < nRows; r++) {
            List<Value> row = new ArrayList<Value>(specs.size());
            for (int i = 0; i < specs.size(); i++) {
                Value v = readValue(c, specs.get(i));
                if (chunkHasNulls && bitmaps[i] != null
                        && (bitmaps[i][r >> 3] & (1 << (r & 7))) != 0) {
                    v = Value.NULL; // 位图为准（占位值已读取以推进游标）
                }
                row.add(v);
            }
            rows.add(java.util.Collections.unmodifiableList(row));
        }
        return rows;
    }

    /** 关闭底层流（提前关闭 = 传输段中止——server 侧停止序列化）。 */
    public void close() throws IOException {
        c.close();
    }

    private static Value readValue(StreamCursor c, ColSpec spec) throws QsdbException {
        int tag = spec.tag;
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
                return spec.tzAware ? Value.of(formatRfc3339(tsNanos)) : Value.of(tsNanos);
            case RowsEncoder.T_DECIMAL128:
                return Value.of(readDecimal128(c, spec.decimalScale));
            default:
                throw c.err("unsupported column type tag " + tag
                        + " in rows payload (retry with readFormat=json)");
        }
    }

    private static String readDecimal128(StreamCursor c, int scale) throws QsdbException {
        byte[] le = c.take(16);
        byte[] be = new byte[16]; // 小端 → 大端二进制补码
        for (int i = 0; i < 16; i++) {
            be[i] = le[15 - i];
        }
        BigInteger unscaled = new BigInteger(be);
        return new BigDecimal(unscaled, scale).toPlainString();
    }

    private static String formatRfc3339(long epochNanos) {
        long secs = Math.floorDiv(epochNanos, 1_000_000_000L);
        long nanos = Math.floorMod(epochNanos, 1_000_000_000L);
        return java.time.format.DateTimeFormatter.ISO_INSTANT
                .format(java.time.Instant.ofEpochSecond(secs, nanos));
    }

    // ------------------------------------------------------------------
    // 流式游标
    // ------------------------------------------------------------------

    /** 顺序读取的小端游标（阻塞读；越界/中断即报错，绝不静默截断）。 */
    private static final class StreamCursor {
        private final InputStream in;
        private long pos;

        StreamCursor(InputStream in) {
            this.in = in;
        }

        QsdbException err(String msg) {
            return new QsdbException(
                    "malformed qsdb-rows stream at offset " + pos + ": " + msg, "HY000");
        }

        /** 读 1 字节；干净 EOF 返回 -1（仅帧边界判尾用）。 */
        int u8OrEof() throws QsdbException {
            try {
                int v = in.read();
                if (v < 0) {
                    return -1;
                }
                pos++;
                return v;
            } catch (IOException e) {
                throw err("io error: " + e.getMessage());
            }
        }

        /** 读 u32；帧边界干净 EOF 返回 -1；半帧 EOF 报截断。 */
        long u32OrEof() throws QsdbException {
            int b0 = u8OrEof();
            if (b0 < 0) {
                return -1;
            }
            int b1 = u8OrEof();
            int b2 = u8OrEof();
            int b3 = u8OrEof();
            if (b1 < 0 || b2 < 0 || b3 < 0) {
                throw err("truncated (row_count frame cut at stream end)");
            }
            return (long) b0 | ((long) b1 << 8) | ((long) b2 << 16) | ((long) b3 << 24);
        }

        int u8() throws QsdbException {
            int v = u8OrEof();
            if (v < 0) {
                throw err("truncated (u8 at stream end)");
            }
            return v;
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

        byte[] take(int n) throws QsdbException {
            byte[] out = new byte[n];
            int read = 0;
            try {
                while (read < n) {
                    int k = in.read(out, read, n - read);
                    if (k < 0) {
                        throw err("truncated (need " + n + " bytes, have " + read + ")");
                    }
                    read += k;
                }
            } catch (IOException e) {
                throw err("io error after " + read + "/" + n + " bytes: " + e.getMessage());
            }
            pos += n;
            return out;
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

        void close() throws IOException {
            in.close();
        }
    }
}
