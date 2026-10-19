package cn.qinwh.qsdb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import cn.qinwh.qsdb.jdbc.internal.Errors;

/**
 * QSDB JDBC 驱动入口（Type 4：纯 Java 直连 HTTP API，无本地库）。
 *
 * <p>URL 形态（设计文档 §四.1）：{@code jdbc:qsdb://host:port[?prop=value&…]}。
 * URL 不含库名（多库用 {@code database} 属性或 {@code setCatalog}）；默认端口 9430。
 *
 * <p>属性（URL 参数优先于 {@code Properties}）：
 * 认证三态——{@code token}（静态 Bearer）或 {@code user}+{@code password}
 * （login 会话）或不设（开放模式匿名）；{@code database}（多库默认库）；
 * {@code connectTimeoutMs}（默认 5000）、{@code queryTimeoutMs}（默认 0 不限）、
 * {@code maxRows}（默认 0 不限）、{@code readFormat}、{@code paramStyle}、
 * {@code transport}（v1 仅 http）、{@code ssl}（v2 预留，true 抛异常）。
 *
 * <p>{@code connect} 不预建网络连接（HTTP 无状态）——首次真实通信在首个
 * 语句或 {@link QsdbConnection#isValid}。SPI 经
 * {@code META-INF/services/java.sql.Driver} 注册，JDBC 4.x 下业务无需
 * {@code Class.forName}。
 */
public class QsdbDriver implements Driver {

    /** URL 前缀。 */
    public static final String URL_PREFIX = "jdbc:qsdb://";

    /** server 默认端口（qsdb server --addr 默认 0.0.0.0:9430）。 */
    public static final int DEFAULT_PORT = 9430;

    static final String DRIVER_NAME = "QSDB JDBC Driver";
    static final String DRIVER_VERSION = "0.1.0";
    static final String DATABASE_PRODUCT_NAME = "QSDB";

    static {
        try {
            java.sql.DriverManager.registerDriver(new QsdbDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        ParsedUrl parsed = ParsedUrl.parse(url);
        Properties props = new Properties();
        if (info != null) {
            props.putAll(info);
        }
        props.putAll(parsed.urlProps);
        return new QsdbConnection(parsed.host, parsed.port, props);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // 面向工具连接对话框的最小属性表（名称/默认值/说明）
        ParsedUrl parsed = ParsedUrl.parse(url != null && acceptsURL(url) ? url : URL_PREFIX + "localhost");
        Properties merged = new Properties();
        if (info != null) {
            merged.putAll(info);
        }
        merged.putAll(parsed.urlProps);
        return new DriverPropertyInfo[] {
                prop("token", merged.getProperty("token"), "static bearer token (server --token)"),
                prop("user", merged.getProperty("user"),
                        "login user (with password; alone = label only)"),
                prop("password", merged.getProperty("password"),
                        "login password (requires user; authenticates via"
                                + " POST /api/v1/auth/login)"),
                prop("database", merged.getProperty("database"),
                        "default database for multi-mount servers (catalog)"),
                prop("connectTimeoutMs", merged.getProperty("connectTimeoutMs", "5000"),
                        "TCP connect timeout in milliseconds"),
                prop("queryTimeoutMs", merged.getProperty("queryTimeoutMs", "0"),
                        "default query timeout in milliseconds (0 = unlimited)"),
                prop("maxRows", merged.getProperty("maxRows", "0"),
                        "default row limit per statement (0 = unlimited)"),
                prop("readFormat", merged.getProperty("readFormat", "auto"),
                        "query response format: auto (prefer binary rows channel,"
                                + " fall back to json on old servers) | json | rows"),
                prop("paramStyle", merged.getProperty("paramStyle", "server"),
                        "prepared-statement parameter channel: server (values bound"
                                + " inside the kernel, never in SQL text) | text"
                                + " (client-side literal rendering, for pre-2026-10 kernels)"),
                prop("batchRewrite", merged.getProperty("batchRewrite", "true"),
                        "collapse executeBatch INSERTs into multi-row VALUES"
                                + " (false = strictly per-statement semantics)"),
                prop("transport", merged.getProperty("transport", "http"),
                        "transport type; only 'http' in v1"),
                prop("ssl", merged.getProperty("ssl", "false"),
                        "TLS; reserved for v2, must be false"),
        };
    }

    private static DriverPropertyInfo prop(String name, String value, String description) {
        DriverPropertyInfo p = new DriverPropertyInfo(name, value);
        p.description = description;
        // 无强制必填项：开放模式可匿名；静态 token 与 user+password 二选一
        p.required = false;
        return p;
    }

    @Override
    public int getMajorVersion() {
        return major();
    }

    @Override
    public int getMinorVersion() {
        return minor();
    }

    static int major() {
        String v = DRIVER_VERSION;
        int dot = v.indexOf('.');
        return Integer.parseInt(dot < 0 ? v : v.substring(0, dot));
    }

    static int minor() {
        String v = DRIVER_VERSION;
        int first = v.indexOf('.');
        if (first < 0) {
            return 0;
        }
        int second = v.indexOf('.', first + 1);
        String rest = second < 0 ? v.substring(first + 1) : v.substring(first + 1, second);
        try {
            return Integer.parseInt(rest);
        } catch (NumberFormatException e) {
            return 0; // "0.1.0-rc1" 等非数字段
        }
    }

    /**
     * 诚实的能力声明（§四.2）：v1 只读、无事务——不吹 {@code true}。
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw Errors.unsupported("java.util.logging is not used by this driver");
    }

    // ------------------------------------------------------------------
    // URL 解析
    // ------------------------------------------------------------------

    static final class ParsedUrl {
        final String host;
        final int port;
        final Properties urlProps = new Properties();

        private ParsedUrl(String host, int port) {
            this.host = host;
            this.port = port;
        }

        /** 解析 {@code jdbc:qsdb://host:port[?k=v&…]}（acceptsURL 已保证前缀）。 */
        static ParsedUrl parse(String url) throws SQLException {
            String rest = url.substring(URL_PREFIX.length());
            if (rest.isEmpty()) {
                throw Errors.invalid("missing host in QSDB URL: '" + url + "'");
            }
            String authority = rest;
            String query = null;
            int q = rest.indexOf('?');
            if (q >= 0) {
                authority = rest.substring(0, q);
                query = rest.substring(q + 1);
            }
            if (authority.contains("/")) {
                throw Errors.invalid("QSDB URL must not contain a path (single database server); got '"
                        + url + "'");
            }
            String host;
            int port = DEFAULT_PORT;
            if (authority.startsWith("[")) {
                // IPv6 字面量 [::1]:9430
                int close = authority.indexOf(']');
                if (close < 0) {
                    throw Errors.invalid("malformed IPv6 host in QSDB URL: '" + url + "'");
                }
                host = authority.substring(1, close);
                String tail = authority.substring(close + 1);
                if (tail.startsWith(":")) {
                    port = parsePort(tail.substring(1), url);
                }
            } else {
                int colon = authority.indexOf(':');
                if (colon >= 0) {
                    host = authority.substring(0, colon);
                    port = parsePort(authority.substring(colon + 1), url);
                } else {
                    host = authority;
                }
            }
            if (host.isEmpty()) {
                throw Errors.invalid("missing host in QSDB URL: '" + url + "'");
            }
            ParsedUrl out = new ParsedUrl(host, port);
            if (query != null && !query.isEmpty()) {
                for (String pair : query.split("&")) {
                    if (pair.isEmpty()) {
                        continue;
                    }
                    int eq = pair.indexOf('=');
                    String k = eq < 0 ? pair : pair.substring(0, eq);
                    String v = eq < 0 ? "" : pair.substring(eq + 1);
                    out.urlProps.setProperty(k.trim(), v.trim());
                }
            }
            return out;
        }

        private static int parsePort(String s, String url) throws SQLException {
            try {
                int port = Integer.parseInt(s);
                if (port < 1 || port > 65535) {
                    throw Errors.invalid("port out of range in QSDB URL: '" + url + "'");
                }
                return port;
            } catch (NumberFormatException e) {
                throw Errors.invalid("malformed port in QSDB URL: '" + url + "'");
            }
        }
    }
}
