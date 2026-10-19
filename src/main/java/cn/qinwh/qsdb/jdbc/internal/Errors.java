package cn.qinwh.qsdb.jdbc.internal;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import cn.qinwh.qsdb.jdbc.QsdbException;

/**
 * 错误 → {@code SQLException} 的统一映射（设计文档 §十）。
 *
 * <p>SQLState 约定：
 * <table border="1">
 *   <tr><th>来源</th><th>SQLState</th></tr>
 *   <tr><td>HTTP 400/404/422 + server error 文本（语法/语义）</td><td>42000</td></tr>
 *   <tr><td>HTTP 401/403（认证失败，token 失效不自愈）</td><td>28000</td></tr>
 *   <tr><td>HTTP 5xx / IO / 网络超时</td><td>08S01（不关闭 Connection——无状态协议）</td></tr>
 *   <tr><td>内核主动终止的查询超时</td><td>HYT00</td></tr>
 *   <tr><td>驱动侧不支持的能力</td><td>0A000（{@link SQLFeatureNotSupportedException}）</td></tr>
 *   <tr><td>驱动侧参数/状态错误</td><td>HY000</td></tr>
 *   <tr><td>值转换失败（字符串转数值等）</td><td>22018</td></tr>
 *   <tr><td>数值溢出</td><td>22003</td></tr>
 * </table>
 *
 * <p>异常 message 一律英文（对齐内核错误风格、便于日志检索）；
 * {@link QsdbException} 的 cause 即原始异常，不做多层包裹。
 *
 * <p>internal API——不在驱动公开面上。
 */
public final class Errors {

    private Errors() {
    }

    /** 语法/语义错误（server 4xx 文本 + HTTP 状态注记）。 */
    public static QsdbException syntax(String serverMessage, int httpStatus) {
        return new QsdbException(serverMessage + " (HTTP " + httpStatus + ")", "42000");
    }

    /** 认证/授权失败。 */
    public static QsdbException auth(String message, int httpStatus) {
        return auth(message, httpStatus, null);
    }

    /** 认证/授权失败（带内核结构化码——UNAUTHORIZED 与 SESSION_REVOKED 靠它分流）。 */
    public static QsdbException auth(String message, int httpStatus, String serverCode) {
        return new QsdbException(message + " (HTTP " + httpStatus + ")", "28000", null, serverCode);
    }

    /** 连接层故障（网络/5xx）——调用方不应关闭 Connection。 */
    public static QsdbException connection(String message, Throwable cause) {
        return connection(message, cause, null);
    }

    /** 连接层故障（带内核结构化码）。 */
    public static QsdbException connection(String message, Throwable cause, String serverCode) {
        QsdbException e = new QsdbException(message, "08S01", cause, serverCode);
        return e;
    }

    /** 查询超时（内核主动终止）。 */
    public static QsdbException timeout(String message) {
        return new QsdbException(message, "HYT00");
    }

    /** 查询超时（带内核结构化码）。 */
    public static QsdbException timeout(String message, String serverCode) {
        return new QsdbException(message, "HYT00", null, serverCode);
    }

    /** 驱动不支持的能力。 */
    public static SQLFeatureNotSupportedException unsupported(String message) {
        return new SQLFeatureNotSupportedException(message, "0A000");
    }

    /** 驱动侧参数/状态错误（消息应带类名与参数索引/列名，可诊断）。 */
    public static SQLException invalid(String message) {
        return new SQLException(message, "HY000");
    }

    /** 驱动侧参数/状态错误（带 cause）。 */
    public static SQLException invalid(String message, Throwable cause) {
        SQLException e = new SQLException(message, "HY000");
        if (cause != null) {
            e.initCause(cause);
        }
        return e;
    }

    /** 值转换失败（字符串 → 数值/日期等，JDBC 22018）。 */
    public static SQLException conversion(String message) {
        return new SQLException(message, "22018");
    }

    /** 数值溢出（JDBC 22003）。 */
    public static SQLException numericOutOfRange(String message) {
        return new SQLException(message, "22003");
    }
}
