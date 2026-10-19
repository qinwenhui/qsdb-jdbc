package cn.qinwh.qsdb.jdbc;

import java.sql.SQLException;

/**
 * QSDB 驱动的基础异常：{@code SQLException} 子类，构造时即携带 SQLState
 * （设计文档 §十的映射约定）与内核结构化错误码（J3 契约的
 * {@code code}，如 {@code SESSION_REVOKED}）。
 *
 * <p>由传输层抛出（协议/网络错误），JDBC SPI 层可直接向上传递——它本身就是
 * 标准异常层级的一员。message 一律英文；不含 token 等敏感信息。
 *
 * <p><b>{@code serverCode} 的用途</b>：SQLState 归驱动所有（内核仅建议），
 * 同一 SQLState 下动作可能相反——如 28000 的 {@code UNAUTHORIZED}（凭证错，
 * 不可自愈）与 {@code SESSION_REVOKED}（会话被踢，重登即恢复）只有 code 能
 * 区分。消费方判分支用 {@link #getServerCode()}，别嗅探 message 文案。
 */
public class QsdbException extends SQLException {

    private static final long serialVersionUID = 1L;

    /** 内核结构化错误码（契约级稳定；旧 server / 传输层错误为 null）。 */
    private final String serverCode;

    public QsdbException(String message, String sqlState) {
        this(message, sqlState, null, null);
    }

    public QsdbException(String message, String sqlState, Throwable cause) {
        this(message, sqlState, cause, null);
    }

    /**
     * @param serverCode 内核结构化错误码（{@code {"error","code"}} 的 code；
     *                   null = 无——旧 server / 客户端本地错误）
     */
    public QsdbException(String message, String sqlState, Throwable cause, String serverCode) {
        super(message, sqlState, cause);
        this.serverCode = serverCode;
    }

    /** 内核结构化错误码（如 {@code SESSION_REVOKED}）；null = 无。 */
    public String getServerCode() {
        return serverCode;
    }
}
