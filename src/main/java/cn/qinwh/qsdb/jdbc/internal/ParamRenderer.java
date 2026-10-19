package cn.qinwh.qsdb.jdbc.internal;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * {@code ?} 占位符 → 类型化 SQL 字面量的渲染器（设计文档 §七，安全核心）。
 *
 * <p><b>本类是驱动中唯一允许生成 SQL 文本的地方</b>——任何绕过它拼接 SQL
 * 的代码都是评审红线（§七.2.5）。
 *
 * <p>规则（§七.2，实现即验收标准）：
 * <ol>
 *   <li>占位符计数带最小 SQL 词法：字符串字面量（{@code '…'}，含 {@code ''}
 *       转义）、{@code --} 行注释、{@code /*} 块注释、双引号/反引号标识符
 *       内的 {@code ?} 不计；参数数与占位符数不符 → 执行前 fail-fast；</li>
 *   <li>类型化字面量：日期/时间带 {@code DATE '…'} / {@code TIME '…'} /
 *       {@code TIMESTAMP '…'} 前缀（裸字符串在内核是 VARCHAR 语义）；</li>
 *   <li>字符串转义 {@code '} → {@code ''}；控制字符（&lt;0x20）渲染为拒绝
 *       （不猜测内核行为）——服务端参数通道（paramStyle=server）不受此限
 *       （JSON 转义合法，见 {@link ParamJson}）；</li>
 *   <li>浮点 NaN/Infinity 拒绝（SQL 端无字面量形态）——params 通道经
 *       字符串形态支持（Rust f64 parse 原生接受）；</li>
 *   <li>BINARY 渲染 {@code X'…'}（内核第九轮起可用）；</li>
 *   <li>{@code setNull} 渲染 {@code NULL}。</li>
 * </ol>
 *
 * <p>internal API——不在驱动公开面上。静态方法无状态，线程安全。
 */
public final class ParamRenderer {

    private ParamRenderer() {
    }

    /**
     * 扫描 SQL 中的占位符个数（跳过字符串/注释/标识符引用内的 {@code ?}）。
     *
     * @throws SQLException HY000：SQL 中的字符串/注释未闭合
     */
    public static int countPlaceholders(String sql) throws SQLException {
        return scan(sql, null, null);
    }

    /**
     * 渲染：把 SQL 中的 {@code ?} 逐个替换为参数的类型化字面量。
     *
     * @param sql    含占位符的 SQL
     * @param params 参数数组（调用前先经 {@link #validate} 校验个数与完备性）
     * @return 渲染后的 SQL
     * @throws SQLException HY000：参数未设置 / 字面量非法；22018：字符串含控制字符
     */
    public static String render(String sql, Param[] params) throws SQLException {
        validate(sql, params);
        StringBuilder out = new StringBuilder(sql.length() + 32);
        scan(sql, out, params);
        return out.toString();
    }

    /** 执行前校验：占位符数与参数数一致（fail-fast，§七.2.1）。 */
    public static void validate(String sql, Param[] params) throws SQLException {
        int expected = countPlaceholders(sql);
        if (expected != params.length) {
            throw Errors.invalid("parameter count mismatch: " + expected
                    + " placeholder(s) in SQL but " + params.length + " parameter(s) set");
        }
        for (int i = 0; i < params.length; i++) {
            if (params[i] == null) {
                throw Errors.invalid("parameter " + (i + 1) + " is not set");
            }
        }
    }

    /**
     * 单遍词法扫描。
     *
     * @param out    非 null 时同步输出替换后的 SQL（占位符 → 字面量）
     * @param params 参数（out 为 null 时忽略）
     * @return 占位符计数
     */
    private static int scan(String sql, StringBuilder out, Param[] params) throws SQLException {
        int count = 0;
        int n = sql.length();
        int i = 0;
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {
                // 字符串字面量：'' 为转义引号
                int j = i + 1;
                boolean closed = false;
                while (j < n) {
                    if (sql.charAt(j) == '\'') {
                        if (j + 1 < n && sql.charAt(j + 1) == '\'') {
                            j += 2;
                            continue;
                        }
                        closed = true;
                        j++;
                        break;
                    }
                    j++;
                }
                if (!closed) {
                    throw Errors.invalid("unclosed string literal in SQL at offset " + i);
                }
                if (out != null) {
                    out.append(sql, i, j);
                }
                i = j;
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                // 行注释：至行尾
                int j = sql.indexOf('\n', i);
                if (j < 0) {
                    j = n;
                }
                if (out != null) {
                    out.append(sql, i, j);
                }
                i = j;
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                // 块注释：至 */（未闭合视为词法错误——sqlparser 同样拒绝）
                int j = sql.indexOf("*/", i + 2);
                if (j < 0) {
                    throw Errors.invalid("unclosed block comment in SQL at offset " + i);
                }
                j += 2;
                if (out != null) {
                    out.append(sql, i, j);
                }
                i = j;
            } else if (c == '"' || c == '`') {
                // 标识符引用（sqlparser Generic 方言支持 "ident" / `ident`）
                int j = sql.indexOf(String.valueOf(c), i + 1);
                if (j < 0) {
                    throw Errors.invalid("unclosed quoted identifier in SQL at offset " + i);
                }
                j++;
                if (out != null) {
                    out.append(sql, i, j);
                }
                i = j;
            } else if (c == '?') {
                count++;
                if (out != null) {
                    out.append(params[count - 1].renderLiteral());
                }
                i++;
            } else {
                if (out != null) {
                    out.append(c);
                }
                i++;
            }
        }
        return count;
    }

    // ------------------------------------------------------------------
    // 参数模型与字面量渲染
    // ------------------------------------------------------------------

    /**
     * 单个参数：JDBC 目标类型 + Java 值。
     * 不可变；由 {@code PreparedStatement.setXxx} 构造。
     */
    public static final class Param {
        public final QsdbType targetType;
        public final Object value;

        private Param(QsdbType targetType, Object value) {
            this.targetType = targetType;
            this.value = value;
        }

        public static Param of(QsdbType type, Object value) {
            return new Param(type, value);
        }

        public static Param nullValue(QsdbType type) {
            return new Param(type, null);
        }

        /** 渲染为类型化 SQL 字面量（§6.2 形态表）。 */
        String renderLiteral() throws SQLException {
            if (value == null) {
                return "NULL";
            }
            switch (targetType) {
                case BIGINT:
                    if (value instanceof Byte) {
                        return value.toString();
                    }
                    if (value instanceof Short) {
                        return value.toString();
                    }
                    if (value instanceof Integer) {
                        return value.toString();
                    }
                    if (value instanceof Long) {
                        return value.toString();
                    }
                    throw conversionError("integer");
                case FLOAT:
                    if (value instanceof Float) {
                        Float f = (Float) value;
                        requireFinite(f.isInfinite() || f.isNaN(), f);
                        return value.toString();
                    }
                    throw conversionError("FLOAT");
                case DOUBLE:
                    if (value instanceof Double) {
                        Double d = (Double) value;
                        requireFinite(d.isInfinite() || d.isNaN(), d);
                        return value.toString();
                    }
                    if (value instanceof Float) {
                        Float f = (Float) value;
                        requireFinite(f.isInfinite() || f.isNaN(), f);
                        return value.toString();
                    }
                    throw conversionError("DOUBLE");
                case DECIMAL:
                    if (value instanceof BigDecimal) {
                        return ((BigDecimal) value).toPlainString(); // 精确十进制串，不走科学计数法
                    }
                    throw conversionError("DECIMAL");
                case BOOLEAN:
                    if (value instanceof Boolean) {
                        return ((Boolean) value) ? "TRUE" : "FALSE";
                    }
                    throw conversionError("BOOLEAN");
                case VARCHAR:
                case TEXT:
                    if (value instanceof String) {
                        return quoteString((String) value);
                    }
                    throw conversionError("VARCHAR");
                case DATE:
                    if (value instanceof Date) {
                        return "DATE '" + ((Date) value).toLocalDate().format(LocalDateISO) + "'";
                    }
                    throw conversionError("DATE");
                case TIME:
                    if (value instanceof LocalTime) {
                        return "TIME '" + formatTime((LocalTime) value) + "'";
                    }
                    if (value instanceof Time) {
                        return "TIME '" + formatTime(((Time) value).toLocalTime()) + "'";
                    }
                    throw conversionError("TIME");
                case TIMESTAMP:
                    if (value instanceof Timestamp) {
                        return "TIMESTAMP '" + formatTimestamp((Timestamp) value) + "'";
                    }
                    throw conversionError("TIMESTAMP");
                case BINARY:
                    // 内核第九轮（5dfc928）起 X'…' 字节串字面量可用（偶数位/
                    // 非 hex 显式报错）——文本渲染通道的 BINARY 参数转正。
                    // 服务端参数通道（paramStyle=server）经 {"type":"BINARY",
                    // "value":"hex"} 走同一内核解析器，两通道值语义一致
                    if (value instanceof byte[]) {
                        return "X'" + hexEncode((byte[]) value) + "'";
                    }
                    throw conversionError("BINARY");
                default:
                    throw Errors.unsupported(
                            "parameter rendering for " + targetType + " is not supported"
                                    + " (INTERVAL has no parameter form in this engine)");
            }
        }

        private void requireFinite(boolean bad, Number v) throws SQLException {
            if (bad) {
                throw Errors.invalid("cannot render " + v + " as SQL literal: NaN/Infinity has no"
                        + " SQL literal form; use a sentinel or CAST in SQL");
            }
        }

        private SQLException conversionError(String target) {
            return Errors.invalid("cannot render value of " + value.getClass().getName()
                    + " as " + target + " literal (setXxx/type mismatch)");
        }
    }

    private static final java.time.format.DateTimeFormatter LocalDateISO =
            java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd");

    /** {@code '…'} 引号转义；控制字符（<0x20）拒绝（§七.2.3）。 */
    static String quoteString(String s) throws SQLException {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) < 0x20) {
                throw Errors.conversion(
                        "string parameter contains control character U+"
                                + String.format("%04X", (int) s.charAt(i))
                                + " at index " + i + "; rendering it as a SQL literal is refused"
                                + " (use a server-side-safe encoding or reject at application layer)");
            }
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('\'');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                sb.append('\'');
            }
            sb.append(c);
        }
        sb.append('\'');
        return sb.toString();
    }

    /** {@code HH:MM:SS[.f{1,9}]}（内核 parse_time_ns 接受形态，§temporal.rs）。 */
    static String formatTime(LocalTime lt) {
        StringBuilder sb = new StringBuilder(19);
        sb.append(String.format("%02d:%02d:%02d", lt.getHour(), lt.getMinute(), lt.getSecond()));
        appendNanos(sb, lt.getNano());
        return sb.toString();
    }

    /** {@code yyyy-MM-dd HH:mm:ss[.f{1,9}]}（内核 parse_timestamp_ns 接受空格分隔）。 */
    static String formatTimestamp(Timestamp ts) {
        LocalDateTime dt = ts.toLocalDateTime();
        StringBuilder sb = new StringBuilder(32);
        sb.append(String.format("%04d-%02d-%02d %02d:%02d:%02d",
                dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                dt.getHour(), dt.getMinute(), dt.getSecond()));
        appendNanos(sb, dt.getNano());
        return sb.toString();
    }

    /** 小数部分：纳秒 &gt;0 时追加 {@code .f…}（去尾零，1~9 位）。 */
    private static void appendNanos(StringBuilder sb, int nanos) {
        if (nanos == 0) {
            return;
        }
        String frac = String.format("%09d", nanos);
        int end = frac.length();
        while (end > 1 && frac.charAt(end - 1) == '0') {
            end--;
        }
        sb.append('.').append(frac, 0, end);
    }

    /** 字节 → 小写 hex（{@code X'…'} 字面量与 params 通道共用形态，内核可逆契约 J2）。 */
    static String hexEncode(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        char[] hex = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xff;
            out[i * 2] = hex[v >>> 4];
            out[i * 2 + 1] = hex[v & 0x0f];
        }
        return new String(out);
    }
}
