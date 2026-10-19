package cn.qinwh.qsdb.jdbc.transport;

import java.util.List;

import cn.qinwh.qsdb.jdbc.QsdbException;

/**
 * Transport SPI——传输层抽象（设计文档 §二 / §三.4）。
 *
 * <p>v1 实现 {@code transport.http.HttpTransport}；v2 的
 * {@code FlightTransport}（JDK 11+ / Arrow IPC）实现同一接口即插即用，
 * JDBC 对外接口零变化。实现要求：
 * <ul>
 *   <li>无连接会话状态——实现对象可以是纯配置持有者；</li>
 *   <li>异常统一抛 {@link QsdbException}（SQLState 已按 §十映射）；</li>
 *   <li>线程模型遵循 JDBC：驱动保证同一实例上的调用串行化。</li>
 * </ul>
 */
public interface Transport extends AutoCloseable {

    /** 执行 SQL（§五.1 契约）；非 SELECT 返回 message 形态响应。 */
    QueryResponse query(String sql, QueryOptions opts) throws QsdbException;

    /**
     * 流式执行（P3：{@code setFetchSize} 真流式——rows 通道 +
     * {@code Connection: close}，逐块消费内存恒定）。
     *
     * <p>SELECT → {@link RowsStream}（{@code stream} 非 null）；
     * 非 SELECT → 内核回 JSON 回执，以 {@code oneShot} 形态交付（stream 为
     * null）。旧 server 不支持流式/格式被拒 → 抛 QsdbException，调用方按
     * {@code readFormat=auto} 语义降级一次性通道。
     */
    StreamedResult queryStream(String sql, QueryOptions opts) throws QsdbException;

    /** 全部表名（§五.2：GET /api/v1/tables）。 */
    List<String> tables() throws QsdbException;

    /** 表 schema（§五.2：GET /api/v1/tables/{t}/schema；表不存在抛 QsdbException）。 */
    TableSchema tableSchema(String table) throws QsdbException;

    /**
     * 全部表名 → schema 的批量视图（内核 J6：{@code GET /tables?withSchema=1}）。
     * （实现注记：接口演进偏差已回写文档 §17.3——消除 getColumns 的 N+1；
     * 传输层不支持批量时实现方可回退为逐表拉取。）
     */
    java.util.Map<String, TableSchema> allTableSchemas() throws QsdbException;

    /**
     * 挂载库清单（内核十六批 M-B：{@code GET /api/v1/databases}；多库面的
     * 目录函数 {@code getCatalogs} 数据源）。非 admin 只列有 grant 的库。
     * 旧 server（无此端点）抛 QsdbException——调用方自行降级（空集）。
     */
    List<DbInfo> databases() throws QsdbException;

    /**
     * 表分区清单（{@code GET /tables/{t}/partitions} 的 {@code detail} 形态；
     * {@code generation} 是 overwrite CAS 写入的期望值来源）。旧 server 无
     * {@code detail} 时返回按名构造的退化行（generation=0）或空集。
     *
     * @param database 目标库（null = 会话默认库）
     * @param table    表名
     */
    List<PartitionInfo> tablePartitions(String database, String table) throws QsdbException;

    /**
     * 批量写入（内核 {@code qsdb-rows} 行式通道：
     * {@code POST /write?table=&mode=&format=rows}）。批量装载绕过 SQL 文本
     * 解析，是百万行级导入的正道；格式见内核 {@code docs/rows-format.md}。
     *
     * @param table   目标表（payload 不含列描述，表结构由此参数决定）
     * @param mode    {@code append}（默认）或 {@code overwrite}
     * @param payload {@code qsdb-rows} 编码体
     * @return 实际写入行数
     */
    long writeBulk(String table, String mode, byte[] payload) throws QsdbException;

    /**
     * 同 {@link #writeBulk(String, String, byte[])}，多库形态（内核十六批
     * M-B：{@code /write} 的 {@code database} 参数——优先级最高；null = 走
     * 会话默认库/服务端 {@code default}）。
     */
    long writeBulk(String database, String table, String mode, byte[] payload)
            throws QsdbException;

    /**
     * 全参形态：多库 + CAS（内核 F24：{@code mode=overwrite} 时
     * {@code expected_generation} 不匹配 → {@code WRITE_CONFLICT} 409/40001，
     * 动作 = 重读最新 generation 再决定；**只支持单分区**——一次请求命中
     * 多分区服务端 400 拒绝）。
     *
     * @param expectedGeneration CAS 期望值；null = 不带（非 CAS overwrite）
     */
    long writeBulk(String database, String table, String mode, Long expectedGeneration,
                   byte[] payload) throws QsdbException;

    /**
     * 存活探测（GET /health）。网络异常/超时返回 false——不抛异常
     * （连接池探针语义，§四.3）。
     */
    boolean healthy(long timeoutMs);

    /**
     * server 版本串（GET /health 响应的 {@code version} 字段）。
     * （实现注记：设计文档 §三.4 的接口无此方法，为
     * {@code DatabaseMetaData.getDatabaseProductVersion} 补充——偏差已回写文档。）
     *
     * @throws QsdbException 网络/协议错误
     */
    String serverVersion() throws QsdbException;

    /**
     * 精确取消当前正在执行的查询（内核 J11：客户端经 {@code X-QSDB-Query-Id}
     * 请求头预注册 id，取消直达 {@code /queries/{id}/cancel}——零扫描零误伤）。
     *
     * @return true = 已对该 id 发起取消；false = 无在执行查询或 server 未采用
     *         客户端 id（调用方可回退 {@link #cancelQueriesMatching}）
     * @throws QsdbException 网络/协议错误（404 之外的取消失败）
     */
    boolean cancelActiveQuery() throws QsdbException;

    /**
     * 取消与给定 SQL 全等（trim 后）的运行中查询（回退路径，供未支持 J11
     * 的 server）。按 SQL 匹配——同 SQL 并发执行时会一并取消（含其它连接），
     * 仅在 {@link #cancelActiveQuery()} 返回 false 时使用。
     *
     * @param sql 要取消的 SQL 文本（与提交时一致）
     * @throws QsdbException 网络/协议错误（查询不存在或已结束为 no-op，不抛）
     */
    void cancelQueriesMatching(String sql) throws QsdbException;

    /** 释放传输层资源（HTTP 无状态：实现为 no-op 亦可）。 */
    @Override
    void close();

    /**
     * 流式执行的交付形态：SELECT 时 {@code stream} 非 null（逐块消费）；
     * 非 SELECT（DDL/DML 回执）时 {@code oneShot} 非 null。
     */
    final class StreamedResult {
        public final RowsStream stream;
        public final QueryResponse oneShot;

        public StreamedResult(RowsStream stream, QueryResponse oneShot) {
            this.stream = stream;
            this.oneShot = oneShot;
        }
    }
}
