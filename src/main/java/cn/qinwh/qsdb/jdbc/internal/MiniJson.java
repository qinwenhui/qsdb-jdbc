package cn.qinwh.qsdb.jdbc.internal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import cn.qinwh.qsdb.jdbc.QsdbException;

/**
 * 零依赖 JSON 解析器（设计文档硬约束：运行时依赖数恒为 0）。
 *
 * <p>能力边界（刻意最小）：RFC 8259 的数据模型子集——object →
 * {@code Map<String,Object>}（保序）、array → {@code List<Object>}、
 * string → {@code String}、number → {@code Long}（i64 内）/ {@code Double}、
 * {@code true}/{@code false}、{@code null}。不支持流式/超大数/注释。
 *
 * <p>仅供驱动内建 HTTP 通道使用（解析失败抛 {@link QsdbException}，
 * SQLState HY000）；重依赖场景（v2 大结果集）走 IPC 二进制通道，不经本
 * 解析器。
 *
 * <p>internal API——不在驱动公开面上。实例无状态，静态方法线程安全。
 */
public final class MiniJson {

    private MiniJson() {
    }

    /**
     * 解析 JSON 文本为 Java 对象树。
     *
     * @throws QsdbException HY000：非法 JSON（消息带位置）
     */
    public static Object parse(String text) throws QsdbException {
        Parser p = new Parser(text);
        p.skipWs();
        Object v = p.readValue();
        p.skipWs();
        if (!p.eof()) {
            throw p.err("trailing characters after JSON value");
        }
        return v;
    }

    /** 顶层 object 便捷读取：解析并断言为 Map。 */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) throws QsdbException {
        Object v = parse(text);
        if (!(v instanceof Map)) {
            throw new QsdbException("expected JSON object in response, got: " + describe(v), "HY000");
        }
        return (Map<String, Object>) v;
    }

    /** 将字符串按 JSON 字符串规则写入 {@code out}（含首尾引号与转义）。 */
    public static void writeString(StringBuilder out, String s) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    out.append("\\\"");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\b':
                    out.append("\\b");
                    break;
                case '\f':
                    out.append("\\f");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        out.append('"');
    }

    private static String describe(Object v) {
        return v == null ? "null" : v.getClass().getSimpleName();
    }

    // ------------------------------------------------------------------
    // 递归下降解析器
    // ------------------------------------------------------------------

    private static final class Parser {
        private final String text;
        private int pos;

        Parser(String text) {
            this.text = text;
        }

        QsdbException err(String msg) {
            return new QsdbException("invalid JSON at offset " + pos + ": " + msg, "HY000");
        }

        boolean eof() {
            return pos >= text.length();
        }

        void skipWs() {
            while (pos < text.length()) {
                char c = text.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    pos++;
                } else {
                    break;
                }
            }
        }

        Object readValue() throws QsdbException {
            if (eof()) {
                throw err("unexpected end of input");
            }
            char c = text.charAt(pos);
            switch (c) {
                case '{':
                    return readObject();
                case '[':
                    return readArray();
                case '"':
                    return readString();
                case 't':
                    expect("true");
                    return Boolean.TRUE;
                case 'f':
                    expect("false");
                    return Boolean.FALSE;
                case 'n':
                    expect("null");
                    return null;
                default:
                    return readNumber();
            }
        }

        private void expect(String literal) throws QsdbException {
            if (!text.startsWith(literal, pos)) {
                throw err("invalid literal");
            }
            pos += literal.length();
        }

        private Map<String, Object> readObject() throws QsdbException {
            Map<String, Object> out = new LinkedHashMap<String, Object>();
            pos++; // '{'
            skipWs();
            if (!eof() && text.charAt(pos) == '}') {
                pos++;
                return out;
            }
            while (true) {
                skipWs();
                if (eof() || text.charAt(pos) != '"') {
                    throw err("expected string key in object");
                }
                String key = readString();
                skipWs();
                if (eof() || text.charAt(pos) != ':') {
                    throw err("expected ':' after object key");
                }
                pos++;
                skipWs();
                out.put(key, readValue());
                skipWs();
                if (eof()) {
                    throw err("unterminated object");
                }
                char c = text.charAt(pos);
                if (c == ',') {
                    pos++;
                    continue;
                }
                if (c == '}') {
                    pos++;
                    return out;
                }
                throw err("expected ',' or '}' in object");
            }
        }

        private List<Object> readArray() throws QsdbException {
            List<Object> out = new ArrayList<Object>();
            pos++; // '['
            skipWs();
            if (!eof() && text.charAt(pos) == ']') {
                pos++;
                return out;
            }
            while (true) {
                skipWs();
                out.add(readValue());
                skipWs();
                if (eof()) {
                    throw err("unterminated array");
                }
                char c = text.charAt(pos);
                if (c == ',') {
                    pos++;
                    continue;
                }
                if (c == ']') {
                    pos++;
                    return out;
                }
                throw err("expected ',' or ']' in array");
            }
        }

        private String readString() throws QsdbException {
            pos++; // '"'
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (eof()) {
                    throw err("unterminated string");
                }
                char c = text.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                if (eof()) {
                    throw err("unterminated escape");
                }
                char e = text.charAt(pos++);
                switch (e) {
                    case '"':
                        sb.append('"');
                        break;
                    case '\\':
                        sb.append('\\');
                        break;
                    case '/':
                        sb.append('/');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case 'f':
                        sb.append('\f');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'u':
                        sb.append(readUnicodeEscape());
                        break;
                    default:
                        throw err("invalid escape '\\" + e + "'");
                }
            }
        }

        private char readUnicodeEscape() throws QsdbException {
            if (pos + 4 > text.length()) {
                throw err("truncated \\u escape");
            }
            int cp;
            try {
                cp = Integer.parseInt(text.substring(pos, pos + 4), 16);
            } catch (NumberFormatException nfe) {
                throw err("invalid \\u escape");
            }
            pos += 4;
            return (char) cp;
        }

        private Object readNumber() throws QsdbException {
            int start = pos;
            if (!eof() && (text.charAt(pos) == '-' || text.charAt(pos) == '+')) {
                pos++;
            }
            boolean isDouble = false;
            while (!eof()) {
                char c = text.charAt(pos);
                if (c >= '0' && c <= '9') {
                    pos++;
                } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                    isDouble = isDouble || c == '.' || c == 'e' || c == 'E';
                    pos++;
                } else {
                    break;
                }
            }
            String num = text.substring(start, pos);
            if (num.isEmpty() || num.equals("-") || num.equals("+")) {
                throw err("invalid number");
            }
            try {
                if (isDouble) {
                    return Double.valueOf(num);
                }
                return Long.valueOf(num); // i64 外的整数（罕见）→ NumberFormatException → Double
            } catch (NumberFormatException longOverflow) {
                try {
                    return Double.valueOf(num);
                } catch (NumberFormatException e) {
                    throw err("invalid number '" + num + "'");
                }
            }
        }
    }
}
