package cn.qinwh.qsdb.jdbc.internal;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * 服务端参数化通道的 JSON 编码器（内核 {@code sql::params} 契约，
 * 2026-10-19 对齐源码 {@code crates/qsdb/src/sql/params.rs}）。
 *
 * <p><b>通道语义</b>：{@code ?} 占位符原样随 SQL 上线（内核解析前用 sqlparser
 * tokenizer 固化成 {@code $n}——字符串/注释/带引号标识符内的 {@code ?} 不算），
 * 参数进 body {@code "params":[…]}，**值从不进入 SQL 文本**——绑定在内核
 * 绑定器里转 AST 字面量节点。注入防护由内核保证，驱动只做类型忠实编码。
 *
 * <p><b>编码规则</b>（对齐内核 {@code json_to_literal} / {@code parse_literal_text}）：
 * <ul>
 *   <li>裸值（推类型）：null/bool/整数(i64)/浮点(f64)/字符串(VARCHAR)——
 *       DECIMAL 等精确类型**必须显式**（裸 JSON number 会经 f64 丢精度）；</li>
 *   <li>显式 {@code {"type":"<DDL 类型名>","value":…}}：value 允许字符串形态
 *       （内核按目标类型解析文本——大数/精确小数/NaN 都走这里）；</li>
 *   <li>BINARY：hex 串（可带 {@code 0x} 前缀，奇数位/非 hex 内核显式拒绝）；</li>
 *   <li>NaN/±Infinity：JSON 无此 number 形态 → 显式 DOUBLE/FLOAT + 字符串
 *       {@code "NaN"}/{@code "Infinity"}/{@code "-Infinity"}（Rust f64 parse
 *       原生接受）；</li>
 *   <li>控制字符字符串：JSON 转义（backslash-u 形态，{@link MiniJson#writeString}）
 *       → VARCHAR 路径原样接受——文本渲染通道拒绝的形态在这里合法；</li>
 *   <li>INTERVAL：内核参数面不支持（{@code parse_literal_text} 拒绝）→
 *       两通道一致显式报错。</li>
 * </ul>
 *
 * <p>internal API——不在驱动公开面上。静态方法无状态，线程安全。
 */
public final class ParamJson {

    private ParamJson() {
    }

    /**
     * 编码参数数组为 body {@code "params"} 的 JSON 数组文本（**含方括号**；
     * 空数组返回 null——调用方约定"全缺省时请求体与从前逐字节相同"）。
     *
     * @throws SQLException 参数无法编码为内核接受的形态
     */
    public static String toJsonArray(ParamRenderer.Param[] params) throws SQLException {
        if (params == null || params.length == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder(params.length * 16).append('[');
        for (int i = 0; i < params.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            encode(sb, params[i], i + 1);
        }
        return sb.append(']').toString();
    }

    private static void encode(StringBuilder sb, ParamRenderer.Param p, int ordinal)
            throws SQLException {
        if (p.value == null) {
            sb.append("null"); // 内核：JSON null → Literal::Null（目标类型不参与）
            return;
        }
        switch (p.targetType) {
            case BIGINT:
                // 整数族 setXxx 统一 BIGINT 目标；Long 即 i64，裸值直推
                sb.append(valueAsLong(p, ordinal));
                return;
            case BOOLEAN:
                sb.append(((Boolean) require(Boolean.class, p, "BOOLEAN")).booleanValue()
                        ? "true" : "false");
                return;
            case VARCHAR:
            case TEXT:
                MiniJson.writeString(sb, (String) require(String.class, p, "VARCHAR"));
                return; // 裸字符串推 VARCHAR；控制字符经 JSON 转义（backslash-u）合法
            case DOUBLE:
                Double d = (Double) require(Double.class, p, "DOUBLE");
                if (d.isNaN() || d.isInfinite()) {
                    typed(sb, "DOUBLE", nonFiniteSpelling(d.doubleValue()));
                } else {
                    sb.append(formatDouble(d.doubleValue()));
                }
                return;
            case FLOAT:
                Float f = (Float) require(Float.class, p, "FLOAT");
                if (f.isNaN() || f.isInfinite()) {
                    typed(sb, "FLOAT", nonFiniteSpelling(f.floatValue()));
                } else {
                    sb.append(formatDouble(f.doubleValue())); // f64 往返精确；内核侧再窄化
                }
                return;
            case DECIMAL:
                BigDecimal bd = (BigDecimal) require(BigDecimal.class, p, "DECIMAL");
                // 裸 JSON number 会经 f64 丢精度——必须显式 + 字符串形态（内核
                // 按字符串精确转换，不过 f64）。scale<0（如 1E+3）先展开为整数
                int scale = Math.max(bd.scale(), 0);
                typed(sb, "DECIMAL(38," + scale + ")", bd.toPlainString());
                return;
            case DATE:
                Date dt = (Date) require(Date.class, p, "DATE");
                typed(sb, "DATE", dt.toLocalDate()
                        .format(java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd")));
                return;
            case TIME:
                // java.sql.Time 是秒精度薄壳；LocalTime 原生携带纳秒——优先
                if (p.value instanceof java.time.LocalTime) {
                    typed(sb, "TIME", ParamRenderer.formatTime(
                            (java.time.LocalTime) p.value));
                } else {
                    typed(sb, "TIME", ParamRenderer.formatTime(
                            ((Time) require(Time.class, p, "TIME")).toLocalTime()));
                }
                return;
            case TIMESTAMP:
                typed(sb, "TIMESTAMP", ParamRenderer.formatTimestamp(
                        (Timestamp) require(Timestamp.class, p, "TIMESTAMP")));
                return;
            case BINARY:
                byte[] bytes = (byte[]) require(byte[].class, p, "BINARY");
                typed(sb, "BINARY", hexEncode(bytes)); // 小写 hex（内核可逆契约，J2）
                return;
            default:
                throw Errors.unsupported("parameter encoding for " + p.targetType
                        + " is not supported by the server params channel"
                        + " (INTERVAL has no parameter form; paramStyle=text also rejects it)");
        }
    }

    /** 显式形态：{@code {"type":…,"value":"…"}}（value 恒字符串——内核按类型解析文本）。 */
    private static void typed(StringBuilder sb, String type, String value) {
        sb.append("{\"type\":");
        MiniJson.writeString(sb, type);
        sb.append(",\"value\":");
        MiniJson.writeString(sb, value);
        sb.append('}');
    }

    private static long valueAsLong(ParamRenderer.Param p, int ordinal) throws SQLException {
        Object v = p.value;
        if (v instanceof Byte) {
            return ((Byte) v).byteValue();
        }
        if (v instanceof Short) {
            return ((Short) v).shortValue();
        }
        if (v instanceof Integer) {
            return ((Integer) v).intValue();
        }
        if (v instanceof Long) {
            return ((Long) v).longValue();
        }
        throw Errors.invalid("parameter " + ordinal + ": cannot encode "
                + v.getClass().getName() + " as integer param");
    }

    /** Rust f64 parse 接受的非有限拼写（内核浮点契约拼写，J7）。 */
    private static String nonFiniteSpelling(double v) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        return v > 0 ? "Infinity" : "-Infinity";
    }

    /**
     * 有限浮点的 JSON number 文本：{@code Double.toString} 的最短往返形态
     * （如 {@code 2.75} / {@code 1.0E10}）——serde_json 解析回同一 f64。
     */
    private static String formatDouble(double v) {
        return Double.toString(v);
    }

    /** 字节 → 小写 hex（内核 JSON 通道 BINARY 的可逆形态，J2 契约）。 */
    private static String hexEncode(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        char[] hex = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xff;
            out[i * 2] = hex[v >>> 4];
            out[i * 2 + 1] = hex[v & 0x0f];
        }
        return new String(out);
    }

    private static Object require(Class<?> expected, ParamRenderer.Param p, String target)
            throws SQLException {
        if (!expected.isInstance(p.value)) {
            throw Errors.invalid("cannot encode value of " + p.value.getClass().getName()
                    + " as " + target + " param (setXxx/type mismatch)");
        }
        return p.value;
    }
}
