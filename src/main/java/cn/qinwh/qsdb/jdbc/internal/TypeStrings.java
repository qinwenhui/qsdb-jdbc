package cn.qinwh.qsdb.jdbc.internal;

import java.sql.SQLException;

import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * 两种 QSDB 类型字符串方言到 {@link QsdbType} 的统一解析（设计文档 §五.3 / §六）。
 *
 * <p>方言来源（均已按内核代码核实）：
 * <table border="1">
 *   <tr><th>方言</th><th>生成方式</th><th>样例</th></tr>
 *   <tr><td>Arrow</td><td>query 响应 {@code schema[].type}（{@code DataType::to_string()}）</td>
 *       <td>{@code Int64}、{@code Utf8}、{@code Timestamp(Nanosecond, None)}、
 *           {@code Decimal128(10, 2)}、{@code Time64(Nanosecond)}、
 *           {@code Timestamp(Nanosecond, Some("UTC"))}</td></tr>
 *   <tr><td>LogicalType</td><td>{@code /tables/{t}/schema} 的 {@code columns[].type}（Display）</td>
 *       <td>{@code BIGINT}、{@code STRING}、{@code VARCHAR(255)}、{@code CHAR(10)}、
 *           {@code DECIMAL(10, 2)}、{@code TIMESTAMP WITH TIME ZONE (UTC)}</td></tr>
 * </table>
 *
 * <p>解析带容错（大小写/空白不敏感、逗号后可带空格），未知类型显式抛
 * {@code SQLException}——诚实报错，不猜测映射。
 *
 * <p>internal API——不在驱动公开面上。
 */
public final class TypeStrings {

    private TypeStrings() {
    }

    /** 解析结果：归一类型 + 精度/标度。 */
    public static final class Parsed {
        public final QsdbType type;
        public final int precision;
        public final int scale;

        Parsed(QsdbType type, int precision, int scale) {
            this.type = type;
            this.precision = precision;
            this.scale = scale;
        }
    }

    /**
     * 解析任意方言的类型串。
     *
     * @param raw 类型串（Arrow 或 LogicalType 方言，null/空白按 VARCHAR 处理）
     * @return 归一结果；DECIMAL 的 precision/scale 来自类型串，其余小数标度为 0
     * @throws SQLException HY000：未知/不支持的类型串（消息带原文）
     */
    public static Parsed parse(String raw) throws SQLException {
        if (raw == null || raw.trim().isEmpty()) {
            return new Parsed(QsdbType.VARCHAR, QsdbType.VARCHAR.getDefaultPrecision(), 0);
        }
        // 归一：去全部空白、大写——两种方言的判别均在归一形态上进行
        String s = raw.replace(" ", "").replace("\t", "").toUpperCase();
        try {
            return parseNormalized(s, raw);
        } catch (SQLException e) {
            throw e;
        } catch (RuntimeException e) {
            throw Errors.invalid("cannot parse column type '" + raw + "': " + e.getMessage());
        }
    }

    private static Parsed parseNormalized(String s, String raw) throws SQLException {
        // ---- 十进制：DECIMAL(p, s)（Logical）/ DECIMAL128(p, s)、DECIMAL256(p, s)（Arrow）----
        if (s.startsWith("DECIMAL")) {
            int[] ps = parsePrecisionScale(s);
            if (ps != null) {
                return new Parsed(QsdbType.DECIMAL, ps[0], ps[1]);
            }
            return new Parsed(QsdbType.DECIMAL, 38, 0);
        }
        // ---- 整数 ----
        if (s.equals("INT8") || s.equals("TINYINT")) {
            return new Parsed(QsdbType.TINYINT, QsdbType.TINYINT.getDefaultPrecision(), 0);
        }
        if (s.equals("INT16") || s.equals("SMALLINT")) {
            return new Parsed(QsdbType.SMALLINT, QsdbType.SMALLINT.getDefaultPrecision(), 0);
        }
        if (s.equals("INT32") || s.equals("INT") || s.equals("INTEGER")) {
            return new Parsed(QsdbType.INT, QsdbType.INT.getDefaultPrecision(), 0);
        }
        if (s.equals("INT64") || s.equals("BIGINT") || s.equals("LONG")) {
            return new Parsed(QsdbType.BIGINT, QsdbType.BIGINT.getDefaultPrecision(), 0);
        }
        // ---- 浮点 ----
        if (s.equals("FLOAT32") || s.equals("FLOAT") || s.equals("REAL")) {
            return new Parsed(QsdbType.FLOAT, QsdbType.FLOAT.getDefaultPrecision(), 0);
        }
        if (s.equals("FLOAT64") || s.equals("DOUBLE")) {
            return new Parsed(QsdbType.DOUBLE, QsdbType.DOUBLE.getDefaultPrecision(), 0);
        }
        // ---- 字符串：Utf8 / LargeUtf8 / Utf8View（Arrow）+ STRING/VARCHAR(n)/CHAR(n)（Logical）----
        if (s.equals("UTF8") || s.equals("LARGEUTF8") || s.equals("UTF8VIEW")
                || s.equals("STRING") || s.equals("TEXT")) {
            return new Parsed(QsdbType.VARCHAR, QsdbType.VARCHAR.getDefaultPrecision(), 0);
        }
        if (s.startsWith("VARCHAR(")) {
            int n = parseSingleInt(s);
            return new Parsed(QsdbType.VARCHAR, n, 0);
        }
        if (s.equals("VARCHAR") || s.equals("CHAR")) {
            // 裸名宽容：驱动自产元数据结果集用 JDBC 名（VARCHAR），生态文档亦常见
            return new Parsed(QsdbType.VARCHAR, QsdbType.VARCHAR.getDefaultPrecision(), 0);
        }
        if (s.startsWith("CHAR(")) {
            int n = parseSingleInt(s);
            return new Parsed(QsdbType.VARCHAR, n, 0);
        }
        // ---- 布尔 ----
        if (s.equals("BOOLEAN") || s.equals("BOOL")) {
            return new Parsed(QsdbType.BOOLEAN, 1, 0);
        }
        // ---- 日期/时间 ----
        if (s.equals("DATE32") || s.equals("DATE64") || s.equals("DATE")) {
            return new Parsed(QsdbType.DATE, QsdbType.DATE.getDefaultPrecision(), 0);
        }
        if (s.startsWith("TIME32(") || s.startsWith("TIME64(") || s.equals("TIME")) {
            return new Parsed(QsdbType.TIME, QsdbType.TIME.getDefaultPrecision(), 0);
        }
        if (s.startsWith("TIMESTAMP(") || s.equals("TIMESTAMP")) {
            return new Parsed(QsdbType.TIMESTAMP, QsdbType.TIMESTAMP.getDefaultPrecision(), 0);
        }
        // TIMESTAMP WITH TIME ZONE (tz)——归一后无空白
        if (s.startsWith("TIMESTAMPWITHTIMEZONE")) {
            return new Parsed(QsdbType.TIMESTAMP, QsdbType.TIMESTAMP.getDefaultPrecision(), 0);
        }
        // ---- 二进制 ----
        if (s.equals("BINARY") || s.equals("LARGEBINARY") || s.equals("BINARYVIEW")) {
            return new Parsed(QsdbType.BINARY, QsdbType.BINARY.getDefaultPrecision(), 0);
        }
        if (s.startsWith("FIXEDSIZEBINARY(")) {
            int n = parseSingleInt(s);
            return new Parsed(QsdbType.BINARY, n, 0);
        }
        // ---- 间隔：JDBC 无对应类型，降级 VARCHAR（Arrow 显示串）----
        if (s.startsWith("INTERVAL")) {
            return new Parsed(QsdbType.INTERVAL, 0, 0);
        }
        // ---- 全 null 列 ----
        if (s.equals("NULL")) {
            return new Parsed(QsdbType.VARCHAR, 0, 0);
        }
        throw Errors.invalid("unsupported column type '" + raw + "' (neither Arrow nor LogicalType dialect)");
    }

    /** 解析 {@code NAME(p, s)} → {p, s}；无括号形态返回 null。 */
    private static int[] parsePrecisionScale(String s) throws SQLException {
        int open = s.indexOf('(');
        if (open < 0 || !s.endsWith(")")) {
            return null;
        }
        String[] parts = s.substring(open + 1, s.length() - 1).split(",");
        if (parts.length != 2) {
            throw Errors.invalid("malformed decimal type '" + s + "' (expected DECIMAL(precision, scale))");
        }
        try {
            return new int[] {Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim())};
        } catch (NumberFormatException e) {
            throw Errors.invalid("malformed decimal type '" + s + "': " + e.getMessage());
        }
    }

    /** 解析 {@code NAME(n)} → n。 */
    private static int parseSingleInt(String s) throws SQLException {
        int open = s.indexOf('(');
        if (open < 0 || !s.endsWith(")")) {
            throw Errors.invalid("malformed parameterized type '" + s + "'");
        }
        try {
            return Integer.parseInt(s.substring(open + 1, s.length() - 1).trim());
        } catch (NumberFormatException e) {
            throw Errors.invalid("malformed parameterized type '" + s + "': " + e.getMessage());
        }
    }
}
