package cn.qinwh.qsdb.jdbc.transport;

import java.util.Collections;
import java.util.List;

/**
 * 查询响应的传输无关模型（设计文档 §三.4 / §五.1 契约）。
 *
 * <p>两种形态：
 * <ul>
 *   <li>查询结果（SELECT）：{@link #isQuery()} true，{@link #getColumns()} /
 *       {@link #getRows()} 可用；</li>
 *   <li>执行回执（DDL/DML 文本回执）：{@code message} 可用，columns/rows 为空。</li>
 * </ul>
 *
 * <p>{@code queryId} 为内核 J4 后透出的查询标识（响应回显——**以它为准**做
 * 取消/诊断：客户端预注册 id 与服务端实际采用 id 在冲突回退时可能不同）。
 *
 * <p>{@code elapsedMs} 为<b>服务端执行耗时</b>（内核十九批Ⅷ起的响应头
 * {@code X-QSDB-Elapsed-Ms}，不含序列化/网络/客户端解码；旧 server 回退
 * body 的 {@code elapsedMs} 字段；两者都无则为 null——不伪装 0）。
 *
 * <p>不可变（rows 列表包装为只读视图）。
 */
public final class QueryResponse {

    private final List<ColMeta> columns;
    private final List<List<Value>> rows;
    private final String message;
    private final Double elapsedMs;
    private final String queryId;
    private final String command;
    private final Long rowsAffected;

    public static QueryResponse ofQuery(List<ColMeta> columns, List<List<Value>> rows,
                                        Double elapsedMs, String queryId) {
        return new QueryResponse(
                Collections.unmodifiableList(columns),
                Collections.unmodifiableList(rows),
                null, elapsedMs, queryId, null, null);
    }

    /**
     * 执行回执（非查询形态）。
     *
     * @param command      DML 命令名（{@code "INSERT"}/{@code "DELETE"}）；
     *                     DDL 为 null（内核 J12：DDL **不出现**这两个字段——
     *                     "没有行数概念" ≠ "影响 0 行"）
     * @param rowsAffected 受影响行数；DDL/旧 server 为 null
     */
    public static QueryResponse ofMessage(String message, Double elapsedMs, String queryId,
                                          String command, Long rowsAffected) {
        return new QueryResponse(
                Collections.<ColMeta>emptyList(),
                Collections.<List<Value>>emptyList(),
                message, elapsedMs, queryId, command, rowsAffected);
    }

    private QueryResponse(List<ColMeta> columns, List<List<Value>> rows, String message,
                          Double elapsedMs, String queryId, String command, Long rowsAffected) {
        this.columns = columns;
        this.rows = rows;
        this.message = message;
        this.elapsedMs = elapsedMs;
        this.queryId = queryId;
        this.command = command;
        this.rowsAffected = rowsAffected;
    }

    /** 是否为查询结果（SELECT）。 */
    public boolean isQuery() {
        return message == null;
    }

    /** 列元数据（查询形态）。 */
    public List<ColMeta> getColumns() {
        return columns;
    }

    /** 数据行（查询形态；内层行列表不可变）。 */
    public List<List<Value>> getRows() {
        return rows;
    }

    /** 执行回执文本（非查询形态）。 */
    public String getMessage() {
        return message;
    }

    /**
     * 服务端执行耗时（毫秒）；旧 server 未报告时为 null——不伪装 0。
     * 诊断/慢查询定位用（驱动扩展面）。
     */
    public Double getElapsedMs() {
        return elapsedMs;
    }

    /** server 侧查询标识（旧版 server 为 null）。 */
    public String getQueryId() {
        return queryId;
    }

    /** DML 命令名（{@code "INSERT"}/{@code "DELETE"}）；DDL 或旧 server 为 null。 */
    public String getCommand() {
        return command;
    }

    /**
     * 受影响行数（内核 J12 结构化字段）；**DDL 与旧 server 为 null**——
     * null 不等于 0，调用方据此区分"没有行数概念"与"影响 0 行"。
     */
    public Long getRowsAffected() {
        return rowsAffected;
    }
}
