package cn.qinwh.qsdb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.qinwh.qsdb.jdbc.internal.BatchMerge;
import cn.qinwh.qsdb.jdbc.internal.Errors;
import cn.qinwh.qsdb.jdbc.transport.ColMeta;
import cn.qinwh.qsdb.jdbc.transport.QueryOptions;
import cn.qinwh.qsdb.jdbc.transport.QueryResponse;
import cn.qinwh.qsdb.jdbc.transport.Transport;

/**
 * 普通语句（设计文档 §四/§十一/§十五）。
 *
 * <p>行为要点：
 * <ul>
 *   <li>SQL 能力面对齐引擎（2026-10-13 定位修订，§十五）：INSERT（VALUES 形态）、
 *       分区粒度 DELETE、DDL（CREATE/DROP/ALTER/TRUNCATE）放行并透传执行；
 *       UPDATE/MERGE/REPLACE/UPSERT 客户端拦截（存储模型无行级更新，内核同样
 *       拒绝）；executeUpdate 从回执解析受影响行数；</li>
 *   <li>写入生效语义：内核无事务模型，每条写语句立即独立生效（与
 *       autoCommit=true 的 JDBC 语义一致）；批内失败不回滚；</li>
 *   <li>{@code setMaxRows(n)}：<b>驱动侧截断</b>（JDBC 语义；不做 SQL LIMIT
 *       改写——改写即注入面）。注意内核 body {@code maxRows} 是结果护栏的
 *       <b>超限拒绝</b>阈值（R1 绝不部分返回），与截断语义相反，故不上线；
 *       仅显式 {@code setMaxRows(0)}（"不限"）时发 {@code maxRows:0} 解除
 *       本请求的服务端护栏；</li>
 *   <li>{@code setQueryTimeout(sec)} → {@code X-QSDB-Timeout-Ms} 头
 *       （服务端侧终止）逐语句覆盖；{@code setFetchSize} no-op（JSON/rows
 *       一次性返回；真流式见 §十一 v2）；</li>
 *   <li>{@code cancel} 精确取消（J11：X-QSDB-Query-Id 预注册，见方法注释）。</li>
 * </ul>
 *
 * <p>按 JDBC 规范非线程安全。
 */
class QsdbStatement implements Statement {

    /**
     * 拒绝集合：引擎明确不支持、且属存储模型级承诺（表格式无行级更新）的
     * 关键字——客户端拦截给出更早更清晰的错误（内核同样会 501 拒绝）。
     * INSERT / DELETE（分区粒度）/ DDL（CREATE/DROP/ALTER/TRUNCATE）已放开
     * ——驱动 SQL 能力面对齐引擎（2026-10-13 定位修订，§十五）。
     */
    private static final Set<String> REJECTED_KEYWORDS =
            new HashSet<String>(Arrays.asList("UPDATE", "MERGE", "REPLACE", "UPSERT"));

    /** DDL 关键字：执行后连接元数据缓存失效（§十一）。 */
    private static final Set<String> DDL_KEYWORDS =
            new HashSet<String>(Arrays.asList("CREATE", "DROP", "ALTER", "TRUNCATE"));

    /** 批量语句列表（addBatch/executeBatch）。 */
    private final List<String> batch = new ArrayList<String>();

    protected final QsdbConnection connection;

    private Integer maxRowsOverride;          // null = 未设置（用连接默认）
    private Integer queryTimeoutSecOverride;  // null = 未设置（用连接默认）
    private Integer fetchSizeOverride;        // null/0 = 一次性；>0 = 流式（P3）
    private boolean closeOnCompletion;
    private boolean closed;
    private QsdbResultSet resultSet;          // 当前结果集（null = 无）
    private boolean executed;
    private boolean lastWasQuery;
    private long lastUpdateCount = -1;        // 最近 DML/DDL 的受影响行数（查询为 -1）
    private String executingSql;              // 取消目标（执行期间非 null）
    private String lastQueryId;               // 最近响应的 server 查询标识（J4，诊断用）
    private Double lastServerElapsedMs;       // 最近响应的服务端执行耗时（十九批Ⅷ；未知为 null）
    private cn.qinwh.qsdb.jdbc.transport.RowsStream pendingStream; // 流式执行交付（attach 前暂存）

    QsdbStatement(QsdbConnection connection) {
        this.connection = connection;
    }

    // ------------------------------------------------------------------
    // 执行
    // ------------------------------------------------------------------

    @Override
    public java.sql.ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        rejectDml(sql, "executeQuery");
        QueryResponse resp = executeResolved(sql, null);
        if (resp == null) {
            return attachStreamedResultSet(); // 流式 SELECT
        }
        if (!resp.isQuery()) {
            throw Errors.invalid("statement did not produce a result set (message: "
                    + resp.getMessage() + "); use execute() for non-query SQL");
        }
        return attachResultSet(resp);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        ensureOpen();
        rejectDml(sql, "execute");
        QueryResponse resp = executeResolved(sql, null);
        if (resp == null) {
            attachStreamedResultSet(); // 流式 SELECT
            return true;
        }
        if (resp.isQuery()) {
            attachResultSet(resp);
            return true;
        }
        resultSet = null;
        return false;
    }

    /**
     * 统一"解析后执行"入口：{@code fetchSize>0} 时流式优先（rows 流式通道，
     * 逐块消费），格式被拒（旧 server/类型不支持/零行旧内核）按 auto 语义
     * 降级一次性通道；否则直接一次性执行。
     *
     * @return 一次性结果；**流式 SELECT 时返回 null**（结果在
     *         {@link #pendingStream}，经 {@link #attachStreamedResultSet()} 交付）
     */
    QueryResponse executeResolved(String sql, String paramsJson) throws SQLException {
        pendingStream = null;
        if (effectiveFetchSize() <= 0) {
            return executeInternal(sql, paramsJson);
        }
        // 流式请求体与一次性通道同构（format 强制 rows、Connection: close）
        boolean liftGuard = maxRowsOverride != null && maxRowsOverride == 0;
        QueryOptions opts = new QueryOptions(effectiveQueryTimeoutMs(), effectiveMaxRows(),
                connection.readFormat(), liftGuard, paramsJson, connection.queryDatabase(),
                effectiveFetchSize());
        try {
            Transport.StreamedResult r = connection.transport().queryStream(sql, opts);
            executed = true;
            lastWasQuery = r.stream != null;
            lastServerElapsedMs = null; // 流式：服务端耗时头随流交付，暂不透出
            if (r.stream != null) {
                pendingStream = r.stream;
                return null;
            }
            lastQueryId = r.oneShot.getQueryId();
            lastServerElapsedMs = r.oneShot.getElapsedMs();
            lastUpdateCount = r.oneShot.isQuery() ? -1 : affectedRows(r.oneShot);
            lastColumns = r.oneShot.isQuery() ? r.oneShot.getColumns() : null;
            return r.oneShot;
        } catch (QsdbException e) {
            connection.markAuthFailedIfUnauthorized(e);
            // auto 降级（与一次性通道同判据）：server 不认识 format / 类型
            // 不支持 / 零行被拒——退一次性 JSON 重试
            if ("rows".equals(connection.readFormat())) {
                throw e; // 强制 rows：不降级
            }
            if (isFormatUnsupported(e) || isTypeUnsupported(e) || isEmptyResultUnsupported(e)) {
                return executeInternal(sql, paramsJson);
            }
            throw e;
        }
    }

    /** 交付流式结果集（pendingStream 必须已就位）。 */
    QsdbResultSet attachStreamedResultSet() throws SQLException {
        if (pendingStream == null) {
            throw Errors.invalid("no streamed result to attach (driver bug)");
        }
        if (resultSet != null) {
            resultSet.close(); // 前一结果集按规范隐式关闭
        }
        QsdbResultSet rs = new QsdbResultSet(pendingStream, effectiveMaxRows(), this);
        pendingStream = null;
        resultSet = rs;
        return rs;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        ensureOpen();
        return executeUpdateRaw(sql);
    }

    /**
     * <b>旧 server 回退路径</b>：从 DML 回执文本解析受影响行数（形态
     * {@code "N rows inserted"} / {@code "N rows deleted"} / {@code "N rows
     * truncated"}；DDL 如 {@code "table created"} 无行数 → 0）。
     *
     * <p>内核 J12 落地后，新 server 走结构化 {@code rowsAffected} 字段
     * （见 {@link #affectedRows}），本方法仅服务无该字段的旧 server。
     * message 文案不做兼容承诺（码表 §六），故解析失败一律回退 0——
     * 不猜测。
     */
    static int parseAffectedRows(String message) {
        if (message == null) {
            return 0;
        }
        Matcher m = AFFECTED_ROWS.matcher(message);
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }

    private static final Pattern AFFECTED_ROWS = Pattern.compile("\\b(\\d+) rows?\\b");

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw Errors.unsupported("generated keys are not supported");
        }
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw Errors.unsupported("generated keys are not supported");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw Errors.unsupported("generated keys are not supported");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw Errors.unsupported("generated keys are not supported");
        }
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw Errors.unsupported("generated keys are not supported");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw Errors.unsupported("generated keys are not supported");
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        ensureOpen();
        return executeUpdateRawLong(sql); // 行数超 int 时不截断（JDBC 4.2 语义）
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw Errors.unsupported("generated keys are not supported");
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        throw Errors.unsupported("generated keys are not supported");
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        int[] counts = executeBatch();
        long[] out = new long[counts.length];
        for (int i = 0; i < counts.length; i++) {
            out[i] = counts[i];
        }
        return out;
    }

    /** 统一执行通道：查询 → 错误映射 → 认证失败标记 → DDL 缓存失效。 */
    QueryResponse executeInternal(String sql) throws SQLException {
        return executeInternal(sql, null);
    }

    /**
     * 统一执行通道（参数化形态）：{@code paramsJson} 为 body
     * {@code "params"} 的预序列化 JSON 数组（服务端参数化通道，P1；null =
     * 无参数）。其余语义与单参版本一致。
     */
    QueryResponse executeInternal(String sql, String paramsJson) throws SQLException {
        if (sql == null) {
            throw Errors.invalid("SQL is null");
        }
        if (sql.trim().isEmpty()) {
            throw Errors.invalid("SQL is empty");
        }
        // 显式 setMaxRows(0)（JDBC"不限"）→ 解除本请求服务端护栏；maxRows=n
        // 保持纯驱动侧截断（内核 maxRows 是超限拒绝阈值，语义相反，不上线）
        boolean liftGuard = maxRowsOverride != null && maxRowsOverride == 0;
        QueryOptions opts = new QueryOptions(effectiveQueryTimeoutMs(), effectiveMaxRows(),
                connection.readFormat(), liftGuard, paramsJson, connection.queryDatabase());
        QueryResponse resp;
        executingSql = sql;
        try {
            resp = connection.transport().query(sql, opts);
        } catch (QsdbException e) {
            connection.markAuthFailedIfUnauthorized(e); // 仅 UNAUTHORIZED 判死（P2 会话面）
            throw e;
        } finally {
            executingSql = null;
        }
        executed = true;
        lastWasQuery = resp.isQuery();
        lastQueryId = resp.getQueryId();
        lastServerElapsedMs = resp.getElapsedMs();
        if (resp.isQuery()) {
            lastUpdateCount = -1;
            lastColumns = resp.getColumns();
        } else {
            // 结构化 rowsAffected 优先（J12），旧 server 回退解析 message 文案；
            // DDL 无行数概念 → 0（JDBC 对 DDL 的 update count 语义）
            lastUpdateCount = affectedRows(resp);
            lastColumns = null;
        }
        // DDL 放行透传（§十五）；CREATE/DROP/ALTER/TRUNCATE 执行后连接元数据
        // 缓存失效——表/列集可能已变
        String firstWord = firstKeyword(sql);
        if (firstWord != null && DDL_KEYWORDS.contains(firstWord)) {
            connection.invalidateMetadataCache();
        }
        return resp;
    }

    QsdbResultSet attachResultSet(QueryResponse resp) throws SQLException {
        if (resultSet != null) {
            resultSet.close(); // 前一结果集按规范隐式关闭
        }
        resultSet = new QsdbResultSet(resp, effectiveMaxRows(), this);
        return resultSet;
    }

    long effectiveMaxRows() {
        return maxRowsOverride != null ? maxRowsOverride : connection.defaultMaxRows();
    }

    long effectiveQueryTimeoutMs() {
        if (queryTimeoutSecOverride != null) {
            return queryTimeoutSecOverride * 1000L; // 显式 0 = 不限（覆盖连接默认）
        }
        return connection.defaultQueryTimeoutMs();
    }

    static void rejectDml(String sql, String method) throws SQLFeatureNotSupportedException {
        String word = firstKeyword(sql);
        if (word != null && REJECTED_KEYWORDS.contains(word)) {
            throw Errors.unsupported(method + " refuses " + word
                    + ": the QSDB storage model has no row-level update"
                    + " (append + partition-granular overwrite); rewrite as partition DELETE"
                    + " or rebuild the partition via the HTTP bulk-write channel");
        }
    }

    /** SQL 首词（剥离前导注释与空白）；无法判定返回 null。 */
    static String firstKeyword(String sql) {
        if (sql == null) {
            return null;
        }
        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                while (i < n && sql.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                if (end < 0) {
                    return null;
                }
                i = end + 2;
            } else {
                break;
            }
        }
        int start = i;
        while (i < n && Character.isLetter(sql.charAt(i))) {
            i++;
        }
        if (i == start) {
            return null;
        }
        return sql.substring(start, i).toUpperCase(java.util.Locale.ROOT);
    }

    // ------------------------------------------------------------------
    // 结果与结果集状态
    // ------------------------------------------------------------------

    @Override
    public java.sql.ResultSet getResultSet() {
        return resultSet;
    }

    /**
     * 最近结果的更新行数（JDBC 规范语义）：结果集为 -1；DML/DDL 为受影响
     * 行数（结构化 {@code rowsAffected} 字段优先，J12；DDL 无行数概念按 0）。
     * 2026-10-19 前此方法对 DML 恒返回 0——与规范不符，已修复。
     */
    @Override
    public int getUpdateCount() {
        if (!executed || lastWasQuery) {
            return -1; // 规范：无结果/当前结果是结果集时 -1
        }
        return (int) Math.min(lastUpdateCount, Integer.MAX_VALUE);
    }

    @Override
    public long getLargeUpdateCount() {
        if (!executed || lastWasQuery) {
            return -1;
        }
        return lastUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (resultSet != null) {
            resultSet.close(); // 规范：隐式关闭当前结果集
        }
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (current == Statement.CLOSE_CURRENT_RESULT) {
            return getMoreResults();
        }
        return false;
    }

    /** 最近一次查询的列元数据（PreparedStatement#getMetaData 用）。 */
    List<ColMeta> lastColumns;

    // ------------------------------------------------------------------
    // 语句属性
    // ------------------------------------------------------------------

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw Errors.invalid("maxRows must be >= 0, got " + max);
        }
        this.maxRowsOverride = max; // 0 = 显式不限（覆盖连接默认）
    }

    @Override
    public int getMaxRows() {
        return maxRowsOverride == null ? 0 : maxRowsOverride;
    }

    @Override
    public int getMaxFieldSize() {
        return 0; // 不限
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (max < 0) {
            throw Errors.invalid("maxFieldSize must be >= 0, got " + max);
        }
        // no-op：字段大小不设限（JSON 全量返回）
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw Errors.invalid("query timeout must be >= 0 seconds, got " + seconds);
        }
        this.queryTimeoutSecOverride = seconds;
    }

    @Override
    public int getQueryTimeout() {
        return queryTimeoutSecOverride == null ? 0 : queryTimeoutSecOverride;
    }

    /**
     * {@code fetchSize > 0} → <b>真流式</b>（P3，内核 rows 流式通道 +
     * {@code Connection: close}）：结果集按块懒拉取，内存 O(一块)，大结果
     * 不再整包进内存；{@code maxRows} 截断时关流断链（剩余传输作废）。
     * {@code 0}（默认）→ 一次性响应（行为与引入流式前完全一致）。分块的
     * 实际粒度由服务端批大小决定——本参数只是"启用流式"的开关，不是每批
     * 行数的硬保证。
     */
    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0) {
            throw Errors.invalid("fetchSize must be >= 0, got " + rows);
        }
        this.fetchSizeOverride = rows;
    }

    @Override
    public int getFetchSize() {
        return fetchSizeOverride == null ? 0 : fetchSizeOverride;
    }

    int effectiveFetchSize() {
        return fetchSizeOverride != null ? fetchSizeOverride : 0;
    }

    /** 流式通道的三类确定性降级信号（与传输层 auto 判据同形）。 */
    private static boolean isFormatUnsupported(QsdbException e) {
        String m = e.getMessage();
        return m != null && m.contains("format")
                && ("0A000".equals(e.getSQLState()) || "42000".equals(e.getSQLState()));
    }

    private static boolean isTypeUnsupported(QsdbException e) {
        String m = e.getMessage();
        return m != null && m.contains("unsupported column type tag");
    }

    private static boolean isEmptyResultUnsupported(QsdbException e) {
        String m = e.getMessage();
        return m != null && m.contains("empty batches");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw Errors.unsupported("only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        // v1 非目标（§十五）：接受设置但不做 JDBC 逃逸改写
    }

    @Override
    public void setPoolable(boolean poolable) {
        // 连接/语句池化由外部池管理；接受设置
    }

    @Override
    public boolean isPoolable() {
        return true;
    }

    @Override
    public void closeOnCompletion() {
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() {
        return closeOnCompletion;
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw Errors.unsupported("positioned updates/cursors are not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        ensureOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        ensureOpen();
    }

    @Override
    public QsdbConnection getConnection() {
        return connection;
    }

    /**
     * 取消当前正在执行的语句。
     *
     * <p>实现（内核 J11 上线后）：query 请求经 {@code X-QSDB-Query-Id} 头
     * 预注册客户端 id，取消直达 {@code /queries/{id}/cancel}——精确、零误伤。
     * 仅当 server 未采用客户端 id 或查询已结束（404）时回退按 SQL 匹配的
     * 尽力而为路径（同 SQL 并发会一并取消）。语句空闲时为 no-op（JDBC
     * 规范语义）。
     */
    @Override
    public void cancel() throws SQLException {
        ensureOpen();
        if (connection.transport().cancelActiveQuery()) {
            return; // 精确取消已发起（执行段）
        }
        // 流式传输段：查询已执行完（cancel 404 是预期）——中止 = 关流断链
        // （两段停止语义；已收行保留为只读数据）
        if (resultSet != null) {
            resultSet.abortStream();
        }
        String sql = executingSql;
        if (sql != null) {
            connection.transport().cancelQueriesMatching(sql); // QsdbException 即 SQLException
        }
    }

    /** 最近一次执行的 server 查询标识（诊断/审计用；旧版 server 为 null）。 */
    String getLastQueryId() {
        return lastQueryId;
    }

    /**
     * 最近一次执行的<b>服务端执行耗时</b>（毫秒；驱动扩展 API，非 JDBC 规范）。
     *
     * <p>来源：内核十九批Ⅷ的统一响应头 {@code X-QSDB-Elapsed-Ms}（仅内核执行
     * 段，不含序列化/网络传输/客户端解码）；旧 server 回退 body 的
     * {@code elapsedMs} 字段；两者都无（旧 server / 尚未执行）为 {@code null}
     * ——不伪装 0。与 {@code cancel()} / 慢查询定位配合使用。
     */
    public Double getServerElapsedMs() {
        return lastServerElapsedMs;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        ensureOpen();
        rejectDml(sql, "addBatch");
        batch.add(sql);
    }

    @Override
    public void clearBatch() {
        batch.clear();
    }

    /**
     * 执行批（2026-10-13 合并优化）。
     *
     * <p><b>合并</b>：连续的 {@code INSERT … VALUES (…)} 条目会被折叠为单条
     * 多行 VALUES 语句下发（按行数/字节分块，见 {@link BatchMerge}）——
     * 内核每条语句一次 append+manifest 提交（~17ms），逐条往返时提交开销
     * 完全压过数据本身。形态不符（非 INSERT、混合语句、含尾随子句等）的
     * 条目原样单独执行，绝不猜测改写。
     *
     * <p><b>返回</b>：{@code int[]} 仍按原批条目逐项对应（合并只影响下发形态，
     * 不影响结果对齐）。
     *
     * <p><b>失败语义</b>：某组失败即抛出，此前已执行的组各自生效（内核无
     * 事务模型）——不回滚；注意合并后同一组内的条目无法逐条归因失败行。
     *
     * <p>大批量（百万行级）写入仍应走 HTTP write（qsdb-rows/bulk 批量通道）：
     * SQL 文本通道有 ~120k 行/秒 的逐行解析绑定地板，二进制通道高一个量级。
     */
    @Override
    public int[] executeBatch() throws SQLException {
        ensureOpen();
        List<String> entries = new ArrayList<String>(batch);
        batch.clear();
        if (entries.isEmpty()) {
            return new int[0];
        }
        int[] results = new int[entries.size()];
        if (!connection.batchRewrite()) {
            // 逃生舱（batchRewrite=false）：逐条执行，保留 JDBC 严格的
            // 逐条 updateCounts / 失败归因语义
            for (int i = 0; i < entries.size(); i++) {
                results[i] = executeUpdateRaw(entries.get(i));
            }
            return results;
        }
        // 按窗口分块处理：组的行数上限即窗口大小，故组不可能跨窗口（每条目
        // 至少贡献 1 行）——语义等价，且把百万级批次的峰值分配（Fragment /
        // 元组串）从 O(批) 降到 O(窗口)
        int window = BatchMerge.MAX_ROWS_PER_STATEMENT;
        for (int base = 0; base < entries.size(); base += window) {
            int end = Math.min(base + window, entries.size());
            for (BatchMerge.Group g : BatchMerge.group(
                    BatchMerge.fragmentsOf(entries.subList(base, end)))) {
                int affected = executeUpdateRaw(g.sql);
                if (g.counts != null) {
                    System.arraycopy(g.counts, 0, results, base + g.entryStart, g.entryCount);
                } else {
                    results[base + g.entryStart] = affected; // 不透明条目：行数执行后才知
                }
            }
        }
        return results;
    }

    /** 执行一条写语句并返回受影响行数（JDBC int 语义；批合并路径复用）。 */
    int executeUpdateRaw(String sql) throws SQLException {
        return (int) Math.min(executeUpdateRawLong(sql), Integer.MAX_VALUE);
    }

    /** 同上，long 语义（{@code executeLargeUpdate} 用；行数超 int 时不截断）。 */
    long executeUpdateRawLong(String sql) throws SQLException {
        rejectDml(sql, "executeUpdate");
        QueryResponse resp = executeInternal(sql);
        if (resp.isQuery()) {
            throw Errors.invalid("statement produces a result set; use executeQuery()");
        }
        return affectedRows(resp);
    }

    /**
     * 受影响行数：**结构化字段优先**（内核 J12 的 {@code rowsAffected}），
     * 旧 server（无该字段）回退解析 message 文案。
     *
     * <p>注意 DDL **不返回**该字段——此时不可当作"影响 0 行"，而是"没有行数
     * 概念"；两者对 {@code executeUpdate} 的返回值恰好都是 0，故回退路径
     * （文案无行数 → 0）自然正确。
     */
    static long affectedRows(QueryResponse resp) {
        Long n = resp.getRowsAffected();
        if (n != null) {
            return n;
        }
        return parseAffectedRows(resp.getMessage()); // 旧 server 回退（J12 前）
    }

    @Override
    public java.sql.ResultSet getGeneratedKeys() throws SQLException {
        throw Errors.unsupported("generated keys are not supported (QSDB has no row-level identity receipt)");
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        if (resultSet != null) {
            resultSet.close(); // 回调 resultSetClosed → 防递归
        }
        connection.unregister(this);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    void resultSetClosed(QsdbResultSet rs) {
        if (resultSet == rs) {
            resultSet = null;
            if (closeOnCompletion) {
                try {
                    close();
                } catch (SQLException ignored) {
                    // closeOnCompletion 的级联关闭失败不再外抛
                }
            }
        }
    }

    protected void ensureOpen() throws SQLException {
        if (closed) {
            throw Errors.invalid("statement is closed");
        }
        if (connection.isClosed()) {
            throw Errors.invalid("connection is closed: " + connection);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw Errors.invalid("does not implement '" + iface.getName() + "'");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
