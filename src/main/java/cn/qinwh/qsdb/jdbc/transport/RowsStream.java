package cn.qinwh.qsdb.jdbc.transport;

import java.util.List;

import cn.qinwh.qsdb.jdbc.QsdbException;

/**
 * 流式查询结果（P3：{@code setFetchSize} 真流式的传输侧形态）。
 *
 * <p>内核 rows 流式契约：请求带 {@code Connection: close}，服务端逐批序列化、
 * 与总行数脱钩；客户端逐块消费，内存 O(一块)。{@link #columns()} 在流一开始
 * 即完整可读（列描述随头下发，不必等首批数据）。
 *
 * <p><b>两段停止语义</b>（与内核/Studio 对齐）：执行段取消走
 * {@code Statement.cancel()}（X-QSDB-Query-Id 精确取消）；传输段（查询已
 * 执行完、正在吐行）取消 = {@link #close()}——关闭底层连接即停止传输，
 * server 侧 socket 关闭后停止序列化。close 后不可再拉块。
 *
 * <p>非 SELECT 语句没有流（内核回 JSON 回执）——传输层以一次性
 * {@link QueryResponse} 形态交付，见 {@code Transport#queryStream}。
 */
public interface RowsStream extends AutoCloseable {

    /** 列元数据（流头解析后即得）。 */
    List<ColMeta> columns();

    /**
     * 下一块数据行；**null = 流结束**。
     *
     * @throws QsdbException HY000：格式非法/流中断（消息带偏移量）
     */
    List<List<Value>> nextChunk() throws QsdbException;

    /**
     * 关闭流（提前关闭 = 传输段中止）。幂等；正常消费到尾后调用是 no-op。
     */
    @Override
    void close();
}
