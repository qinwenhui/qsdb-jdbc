package cn.qinwh.qsdb.jdbc.internal;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.List;

import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * {@code qsdb-rows} 行式零依赖批量格式的编码器（写侧）。
 *
 * <p>格式权威源：内核 {@code docs/rows-format.md}（契约级稳定）。要点：
 * <ul>
 *   <li>头：{@code "QSDBROWS"} + version u16 + flags u8 + n_cols u16；
 *       <b>写侧 flags 必须为 0</b>（不带列描述——表结构由 {@code table=}
 *       查询参数决定，payload 里再放一份就是两个真相源，内核显式 400 拒绝）</li>
 *   <li>块 × N：{@code row_count u32} + {@code chunk_flags u8} +
 *       （HAS_NULLS 时 n_cols × ceil(row_count/8) 位图，列序拼接）+ 值（行主序）</li>
 *   <li>NULL：位图为准，**值仍照常占位**（定长写全 0、变长写 len=0）——
 *       于是"空串"与"NULL"由位图区分，驱动不用猜</li>
 * </ul>
 *
 * <p>全小端；无对齐要求。写法是一遍顺序追加，**零转置**（这正是行式格式
 * 对"按行取值"的 JDBC 的价值）。
 *
 * <p>internal API——不在驱动公开面上。
 */
public final class RowsEncoder {

    /** 头 magic（8 字节 ASCII）。 */
    public static final byte[] MAGIC = "QSDBROWS".getBytes(StandardCharsets.US_ASCII);

    /** 当前格式版本。 */
    public static final int VERSION = 1;

    // 类型标签（与 bulk 的 1..7 同义同宽）
    public static final int T_BOOLEAN = 1;
    public static final int T_INT32 = 2;
    public static final int T_INT64 = 3;
    public static final int T_FLOAT64 = 4;
    public static final int T_UTF8 = 5;
    public static final int T_DATE = 6;
    public static final int T_TIMESTAMP = 7;
    public static final int T_INT8 = 8;
    public static final int T_INT16 = 9;
    public static final int T_FLOAT32 = 10;
    public static final int T_BINARY = 11;
    public static final int T_DECIMAL128 = 12;
    public static final int T_TIME = 13;
    public static final int T_INTERVAL = 14;

    /**
     * 列编码描述。写侧不发列描述，故 {@code name} 仅用于诊断；
     * 编码本身只需 {@code type}（与 DECIMAL 的 {@code scale}）。
     */
    public static final class Column {
        public final String name;
        public final QsdbType type;
        /** DECIMAL 的标度（未缩放整数 = 值 × 10^scale）。 */
        public final int scale;

        public Column(String name, QsdbType type, int scale) {
            this.name = name;
            this.type = type;
            this.scale = scale;
        }
    }

    private RowsEncoder() {
    }

    /** 该 QsdbType 在 rows 格式中的类型标签（写侧不发，供读侧/诊断与测试）。 */
    public static int typeTag(QsdbType t) {
        switch (t) {
            case BOOLEAN:
                return T_BOOLEAN;
            case TINYINT:
                return T_INT8;
            case SMALLINT:
                return T_INT16;
            case INT:
                return T_INT32;
            case BIGINT:
                return T_INT64;
            case FLOAT:
                return T_FLOAT32;
            case DOUBLE:
                return T_FLOAT64;
            case DECIMAL:
                return T_DECIMAL128;
            case VARCHAR:
            case TEXT:
                return T_UTF8;
            case BINARY:
                return T_BINARY;
            case DATE:
                return T_DATE;
            case TIME:
                return T_TIME;
            case TIMESTAMP:
                return T_TIMESTAMP;
            case INTERVAL:
                return T_INTERVAL;
            default:
                throw new IllegalStateException("unmapped type: " + t);
        }
    }

    /**
     * 编码一个完整 payload（头 + N 个块）。
     *
     * @param columns 列描述（= 表列序，含分区列）
     * @param chunks  每个块的行集；每行是与 {@code columns} 等长的值数组
     *                （canonical 形态见 {@link #writeValue}；null 表示 NULL）
     */
    public static byte[] encode(List<Column> columns, List<List<Object[]>> chunks) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
        writeHeader(out, columns.size());
        for (List<Object[]> rows : chunks) {
            writeChunk(out, columns, rows);
        }
        return out.toByteArray();
    }

    /** 编码头（写侧 flags=0）。 */
    public static void writeHeader(ByteArrayOutputStream out, int nCols) {
        out.write(MAGIC, 0, MAGIC.length);
        writeU16(out, VERSION);
        out.write(0); // flags = 0：写侧不带 schema（内核显式要求）
        writeU16(out, nCols);
    }

    /**
     * 编码一个块。
     *
     * @param cols 列描述
     * @param rows 行集（每行长度必须等于 cols.size()）
     */
    public static void writeChunk(ByteArrayOutputStream out, List<Column> cols, List<Object[]> rows) {
        int nCols = cols.size();
        int nRows = rows.size();
        writeU32(out, nRows);

        // 是否有 NULL：决定 HAS_NULLS 与整段位图
        boolean hasNulls = false;
        for (Object[] row : rows) {
            for (int c = 0; c < nCols; c++) {
                if (row[c] == null) {
                    hasNulls = true;
                    break;
                }
            }
            if (hasNulls) {
                break;
            }
        }
        out.write(hasNulls ? 1 : 0);

        if (hasNulls) {
            // n_cols 段位图，列序拼接，每段 ceil(n_rows/8) 字节，LSB first
            int bitmapBytes = (nRows + 7) / 8;
            for (int c = 0; c < nCols; c++) {
                byte[] bits = new byte[bitmapBytes];
                for (int r = 0; r < nRows; r++) {
                    if (rows.get(r)[c] == null) {
                        bits[r >> 3] |= (byte) (1 << (r & 7));
                    }
                }
                out.write(bits, 0, bits.length);
            }
        }

        // 值：行主序
        for (Object[] row : rows) {
            for (int c = 0; c < nCols; c++) {
                writeValue(out, cols.get(c), row[c]);
            }
        }
    }

    /**
     * 写单个值。canonical 形态（由 {@code QsdbBulkWriter} 在 setXxx 时归一）：
     * {@code null} / {@link Boolean} / {@link Long}（整数；DATE=天数、
     * TIME=自午夜纳秒、TIMESTAMP=自 epoch 纳秒）/ {@link Double} /
     * {@link BigDecimal} / {@link String} / {@code byte[]}。
     *
     * <p>NULL 占位照写（定长全 0、变长 len=0）——解码端按位图还原。
     */
    static void writeValue(ByteArrayOutputStream out, Column col, Object v) {
        switch (col.type) {
            case BOOLEAN:
                out.write(Boolean.TRUE.equals(v) ? 1 : 0);
                return;
            case TINYINT:
                out.write(v == null ? 0 : (int) (long) (Long) v);
                return;
            case SMALLINT:
                writeU16(out, v == null ? 0 : (int) (long) (Long) v);
                return;
            case INT:
                writeU32(out, v == null ? 0L : (Long) v);
                return;
            case BIGINT:
                writeU64(out, v == null ? 0L : (Long) v);
                return;
            case FLOAT:
                writeU32(out, v == null ? 0L : Float.floatToIntBits(((Double) v).floatValue()) & 0xFFFFFFFFL);
                return;
            case DOUBLE:
                writeU64(out, v == null ? 0L : Double.doubleToLongBits((Double) v));
                return;
            case DECIMAL:
                writeDecimal128(out, v == null ? null : (BigDecimal) v, col.scale);
                return;
            case VARCHAR:
            case TEXT:
                writeVarLen(out, v == null ? null : ((String) v).getBytes(StandardCharsets.UTF_8));
                return;
            case BINARY:
                writeVarLen(out, v == null ? null : (byte[]) v);
                return;
            case DATE:
                writeU32(out, v == null ? 0L : ((Long) v) & 0xFFFFFFFFL); // i32 天数
                return;
            case TIME:
            case TIMESTAMP:
                writeU64(out, v == null ? 0L : (Long) v); // i64 纳秒
                return;
            case INTERVAL:
                // 月/日/纳秒三字段；驱动侧不可由用户构造非 NULL 值，故仅占位
                writeU32(out, 0L);
                writeU32(out, 0L);
                writeU64(out, 0L);
                return;
            default:
                throw new IllegalStateException("unmapped type for encoding: " + col.type);
        }
    }

    /** 变长（Utf8/Binary）：u32 小端长度 + 字节；NULL = 长度 0。 */
    private static void writeVarLen(ByteArrayOutputStream out, byte[] bytes) {
        if (bytes == null) {
            writeU32(out, 0);
            return;
        }
        writeU32(out, bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    /**
     * Decimal128：16 字节 i128 小端（**未缩放整数**）。
     * 标度不符时**显式拒绝**（不静默四舍五入——那会改数据）。
     */
    private static void writeDecimal128(ByteArrayOutputStream out, BigDecimal v, int scale) {
        if (v == null) {
            for (int i = 0; i < 16; i++) {
                out.write(0);
            }
            return;
        }
        BigDecimal scaled;
        try {
            scaled = v.setScale(scale, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("decimal value " + v.toPlainString()
                    + " does not fit column scale " + scale
                    + " without rounding (refused rather than silently rounded)");
        }
        BigInteger unscaled = scaled.unscaledValue();
        byte[] raw = unscaled.toByteArray(); // 大端二进制补码（最短）
        if (raw.length > 16) {
            throw new IllegalArgumentException("decimal value " + v.toPlainString()
                    + " exceeds Decimal128 range");
        }
        byte[] be = new byte[16];
        java.util.Arrays.fill(be, (byte) (unscaled.signum() < 0 ? 0xFF : 0x00)); // 符号扩展
        System.arraycopy(raw, 0, be, 16 - raw.length, raw.length);
        for (int i = 15; i >= 0; i--) {
            out.write(be[i]); // 大端 → 小端
        }
    }

    // ---- 小端原语 ----

    static void writeU16(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    static void writeU32(ByteArrayOutputStream out, long v) {
        out.write((int) (v & 0xFF));
        out.write((int) ((v >>> 8) & 0xFF));
        out.write((int) ((v >>> 16) & 0xFF));
        out.write((int) ((v >>> 24) & 0xFF));
    }

    static void writeU64(ByteArrayOutputStream out, long v) {
        writeU32(out, v & 0xFFFFFFFFL);
        writeU32(out, (v >>> 32) & 0xFFFFFFFFL);
    }
}
