package cn.qinwh.qsdb.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import cn.qinwh.qsdb.jdbc.internal.Errors;
import cn.qinwh.qsdb.jdbc.transport.TableSchema;
import cn.qinwh.qsdb.jdbc.transport.Transport;
import cn.qinwh.qsdb.jdbc.transport.http.HttpTransport;

/**
 * QSDB 逻辑连接（设计文档 §四 / §九）。
 *
 * <p>HTTP 无连接会话状态——本对象只是「属性 + 元数据缓存 + 语句簿记」：
 * <ul>
 *   <li>{@code close()} 释放缓存并标记失效（幂等）；</li>
 *   <li>通信异常<b>不</b>使连接失效（无状态协议下下次调用仍可用，§十）；</li>
 *   <li>认证语义（P2 规范化）：{@code UNAUTHORIZED}（凭证错）标记不可用——
 *       不会自愈；{@code SESSION_REVOKED} 由传输层自动重登恢复；
 *       {@code FORBIDDEN} 是授权不足，连接仍可用于其它操作；</li>
 *   <li>多库（P2）：catalog ↔ database 映射（{@code setCatalog}/连接属性
 *       {@code database} → 每查询 body {@code database}，优先级最高）；</li>
 *   <li>元数据缓存（tables/tableSchema）在本连接内复用，首个 DDL 类
 *       execute 后失效（§十一）；</li>
 *   <li>事务语义为只读库定案（§九）：autoCommit 恒 true，
 *       {@code setAutoCommit(false)}/{@code commit()}/{@code rollback()}
 *       显式拒绝而非假成功。</li>
 * </ul>
 *
 * <p>按 JDBC 规范非线程安全。{@code toString()} 不含 token（评审红线）。
 */
public final class QsdbConnection implements Connection {

    private final String url; // 不含查询参数（脱敏展示用）
    private final Transport transport;
    private final int defaultQueryTimeoutMs;
    private final int defaultMaxRows;
    private final boolean batchRewrite; // executeBatch 合并为多行 VALUES（§17.8）

    private boolean closed;
    private boolean authFailed; // 认证失败（UNAUTHORIZED）后标记：凭证错不会自愈
    private boolean readOnlyHint; // setReadOnly 提示（默认 false=可写；JDBC 优化器提示语义）
    private final String readFormat; // auto | json | rows（查询响应格式）
    private final boolean serverParams; // true = 服务端参数化通道（P1 默认）
    private final String defaultDatabase; // 连接属性 database（null = 服务端 default）
    private String currentCatalog; // setCatalog 状态（null = 未设置；P2 多库）
    private final List<QsdbStatement> statements = new ArrayList<QsdbStatement>();
    private QsdbDatabaseMetaData metadata; // 惰性单例

    // 连接级元数据缓存（§十一；本连接内非线程安全使用）
    private List<String> cachedTables;
    private final Map<String, TableSchema> cachedSchemas = new HashMap<String, TableSchema>();
    private Map<String, TableSchema> allSchemas; // J6 批量视图（null = 未拉取）

    QsdbConnection(String host, int port, Properties props) throws SQLException {
        this.url = QsdbDriver.URL_PREFIX + host + ":" + port;
        int connectTimeoutMs = intProp(props, "connectTimeoutMs", 5000);
        this.defaultQueryTimeoutMs = intProp(props, "queryTimeoutMs", 0);
        this.defaultMaxRows = intProp(props, "maxRows", 0);
        this.batchRewrite = boolProp(props, "batchRewrite", true);
        this.readFormat = readFormatProp(props);
        this.serverParams = paramStyleProp(props);
        String transportKind = props.getProperty("transport", "http");
        if (!"http".equalsIgnoreCase(transportKind)) {
            throw new SQLFeatureNotSupportedException("transport '" + transportKind
                    + "' is not supported by this driver build (v1 ships http only;"
                    + " flight is reserved for v2 / JDK 11+)", "0A000");
        }
        if (Boolean.parseBoolean(props.getProperty("ssl", "false"))) {
            throw new SQLFeatureNotSupportedException("ssl=true is reserved for v2;"
                    + " this build only speaks plain HTTP", "0A000");
        }
        // 认证三态（2026-10-19 P2 规范化，无遗留别名）：
        //   token                         → 静态 Bearer（server --token）
        //   user + password               → login 会话（Basic 换进程态 token，
        //                                    SESSION_REVOKED 自动重登分档）
        //   都不设                         → 匿名（server 开放模式）
        // password 必须与 user 成对（密码≠token）；user 可单独存在（标签用途，
        // 供连接池/日志识别，不参与认证）
        String staticToken = props.getProperty("token");
        String user = trimToNull(props.getProperty("user"));
        String password = trimToNull(props.getProperty("password"));
        if (password != null && user == null) {
            throw Errors.invalid("connection property 'password' requires 'user'"
                    + " (user+password authenticates via server login;"
                    + " use 'token' for a static bearer token)");
        }
        this.defaultDatabase = trimToNull(props.getProperty("database"));
        if (defaultDatabase != null && !defaultDatabase.matches("[a-z_][a-z0-9_]*")) {
            throw Errors.invalid("property 'database' must be a mounted database name"
                    + " (lowercase [a-z_][a-z0-9_]*), got '" + defaultDatabase + "'");
        }
        this.transport = new HttpTransport(host, port, staticToken, user, password,
                connectTimeoutMs, defaultQueryTimeoutMs);
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    /**
     * {@code readFormat} 属性：{@code auto}（默认，优先 rows，旧 server 自动
     * 退回 json）| {@code json}（强制 JSON——稳定路径，行为与引入 rows 前完全
     * 一致）| {@code rows}（强制二进制行式通道，失败即报错不降级）。
     */
    private static String readFormatProp(Properties props) throws SQLException {
        String v = props.getProperty("readFormat");
        if (v == null || v.trim().isEmpty()) {
            return "auto";
        }
        String s = v.trim().toLowerCase(java.util.Locale.ROOT);
        if ("auto".equals(s) || "json".equals(s) || "rows".equals(s)) {
            return s;
        }
        throw Errors.invalid("property 'readFormat' must be auto|json|rows, got '" + v + "'");
    }

    /** 查询响应格式策略（auto|json|rows），传输层据此选择并回退。 */
    String readFormat() {
        return readFormat;
    }

    /**
     * {@code paramStyle} 属性：{@code server}（默认——值经 body
     * {@code params} 进内核绑定器，<b>值从不进入 SQL 文本</b>；注入防护由
     * 内核保证，且 BINARY/NaN/控制字符参数全部可用）| {@code text}（回退
     * 客户端字面量渲染——服务 2026-10 之前的旧内核，{@code setBytes} 走
     * {@code X'…'} 字面量、NaN/控制字符拒绝）。仅 PreparedStatement 生效。
     */
    private static boolean paramStyleProp(Properties props) throws SQLException {
        String v = props.getProperty("paramStyle");
        if (v == null || v.trim().isEmpty()) {
            return true; // server（默认）
        }
        String s = v.trim().toLowerCase(java.util.Locale.ROOT);
        if ("server".equals(s)) {
            return true;
        }
        if ("text".equals(s)) {
            return false;
        }
        throw Errors.invalid("property 'paramStyle' must be server|text, got '" + v + "'");
    }

    /** 是否用服务端参数化通道（paramStyle=server，默认）。 */
    boolean serverParams() {
        return serverParams;
    }

    /**
     * 查询目标库（P2 多库）：{@code setCatalog} 显式值 &gt; 连接属性
     * {@code database} &gt; null（不发字段——服务端按会话默认库/`default` 裁决）。
     * 与内核 body `database` 的优先级（显式 &gt; 会话默认 &gt; default）逐层对齐。
     */
    String queryDatabase() {
        return currentCatalog != null ? currentCatalog : defaultDatabase;
    }

    /**
     * 连接属性 {@code database}（多库部署的默认库；null = 未指定）。
     */
    String defaultDatabase() {
        return defaultDatabase;
    }

    /** 登录会话角色（login 连接才有；admin/writer/reader）。 */
    String sessionRole() {
        return transport instanceof HttpTransport
                ? ((HttpTransport) transport).sessionRole() : null;
    }

    /** 会话 id（login 连接才有）。 */
    String sessionId() {
        return transport instanceof HttpTransport
                ? ((HttpTransport) transport).sessionId() : null;
    }

    /**
     * 布尔属性解析：严格——拼错即报错（对齐内核「不静默兜底」协议原则）。
     * 只接受 true/false（大小写不敏感）。
     */
    private static boolean boolProp(Properties props, String key, boolean dflt) throws SQLException {
        String v = props.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return dflt;
        }
        String s = v.trim();
        if ("true".equalsIgnoreCase(s)) {
            return true;
        }
        if ("false".equalsIgnoreCase(s)) {
            return false;
        }
        throw Errors.invalid("property '" + key + "' must be true or false, got '" + v + "'");
    }

    private static int intProp(Properties props, String key, int dflt) throws SQLException {
        String v = props.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return dflt;
        }
        try {
            int n = Integer.parseInt(v.trim());
            if (n < 0) {
                throw Errors.invalid("property '" + key + "' must be >= 0, got " + n);
            }
            return n;
        } catch (NumberFormatException e) {
            throw Errors.invalid("property '" + key + "' is not a valid integer: '" + v + "'");
        }
    }

    // ------------------------------------------------------------------
    // 内部协作面（Statement / DatabaseMetaData 使用）
    // ------------------------------------------------------------------

    /** 脱敏 URL（jdbc:qsdb://host:port，无查询参数/token）。 */
    String url() {
        return url;
    }

    Transport transport() {
        return transport;
    }

    /** server 版本（/health 的 version 字段；失败返回 null，调用方自定回退）。 */
    String serverVersion() {
        try {
            return transport.serverVersion();
        } catch (cn.qinwh.qsdb.jdbc.QsdbException e) {
            return null;
        }
    }

    int defaultQueryTimeoutMs() {
        return defaultQueryTimeoutMs;
    }

    int defaultMaxRows() {
        return defaultMaxRows;
    }

    /**
     * 是否允许 executeBatch 把连续 INSERT 合并为多行 VALUES（§17.8）。
     * 默认 true（大批量装载收益数百倍）；置 false 回退逐条执行——需要
     * JDBC 严格的逐条 {@code getUpdateCounts()} / 失败归因语义时使用。
     */
    boolean batchRewrite() {
        return batchRewrite;
    }

    void markAuthFailed() {
        this.authFailed = true;
    }

    boolean isAuthFailed() {
        return authFailed;
    }

    void register(QsdbStatement stmt) {
        statements.add(stmt);
    }

    void unregister(QsdbStatement stmt) {
        statements.remove(stmt);
    }

    void invalidateMetadataCache() {
        cachedTables = null;
        cachedSchemas.clear();
        allSchemas = null;
    }

    /** 全部表名（缓存优先；DatabaseMetaData 走此路）。 */
    List<String> cachedTables() throws SQLException {
        checkOpen();
        if (cachedTables == null) {
            try {
                cachedTables = transport.tables();
            } catch (QsdbException e) {
                markAuthFailedIfUnauthorized(e);
                throw e;
            }
        }
        return cachedTables;
    }

    /**
     * 只有 {@code UNAUTHORIZED}（凭证错，不可自愈）才标记连接永久不可用；
     * {@code SESSION_REVOKED}（会话被踢）由传输层重登恢复；{@code FORBIDDEN}
     * 是授权不足而非凭证错（换库/等授权后同连接仍可用）——P2 起不再一并判死。
     */
    void markAuthFailedIfUnauthorized(QsdbException e) {
        if ("UNAUTHORIZED".equals(e.getServerCode())
                // 旧 server 无结构化 code：401 语义即凭证错，保守标记
                || (e.getServerCode() == null && e.getSQLState() != null
                        && e.getSQLState().startsWith("28"))) {
            markAuthFailed();
        }
    }

    /** 表 schema（缓存优先；DatabaseMetaData 走此路）。 */
    TableSchema cachedTableSchema(String table) throws SQLException {
        checkOpen();
        TableSchema ts = cachedSchemas.get(table);
        if (ts == null) {
            ts = transport.tableSchema(table);
            cachedSchemas.put(table, ts);
        }
        return ts;
    }

    /**
     * 全部表的 schema 批量视图（J6：{@code GET /tables?withSchema=1} 一次
     * 拉全，消除 getColumns 的 N+1；连接内整体缓存，DDL 后失效）。
     * 批量端点不可用（旧 server）时回退逐表拉取。
     */
    Map<String, TableSchema> allSchemas() throws SQLException {
        checkOpen();
        if (allSchemas == null) {
            allSchemas = transport.allTableSchemas();
            cachedTables = new ArrayList<String>(allSchemas.keySet());
            cachedSchemas.putAll(allSchemas);
        }
        return allSchemas;
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw Errors.invalid("connection is closed: " + url);
        }
        if (authFailed) {
            throw Errors.auth("authentication previously failed (UNAUTHORIZED);"
                    + " bad credentials will not self-heal — create a new connection"
                    + " with valid credentials", 401);
        }
    }

    // ------------------------------------------------------------------
    // 语句工厂
    // ------------------------------------------------------------------

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkOpen();
        checkCursorShape(resultSetType, resultSetConcurrency, resultSetHoldability);
        QsdbStatement stmt = new QsdbStatement(this);
        register(stmt);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        checkOpen();
        checkCursorShape(resultSetType, resultSetConcurrency, resultSetHoldability);
        QsdbPreparedStatement stmt = new QsdbPreparedStatement(this, sql);
        register(stmt);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw Errors.unsupported("generated keys are not supported (QSDB has no row-level identity receipt)");
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw Errors.unsupported("generated keys are not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw Errors.unsupported("generated keys are not supported");
    }

    /**
     * 表分区清单（驱动扩展 API，非 JDBC 规范；{@code GET /tables/{t}/partitions}
     * 的 detail 形态）。主要用途：overwrite CAS 写入的 {@code generation} 期望值
     * 来源（{@code QsdbBulkWriter#overwriteCas}）——动作语义见
     * {@code WRITE_CONFLICT}（40001：重读最新 generation 再决定，勿盲重试）。
     *
     * @param table 表名（可用 {@code db.t} 限定多库形态）
     */
    public java.util.List<cn.qinwh.qsdb.jdbc.transport.PartitionInfo> getTablePartitions(
            String table) throws SQLException {
        checkOpen();
        try {
            return transport.tablePartitions(currentCatalog, table);
        } catch (QsdbException e) {
            markAuthFailedIfUnauthorized(e);
            throw e;
        }
    }

    /**
     * 创建批量写入器（大批量装载专用；走内核 {@code qsdb-rows} 行式通道）。
     *
     * <p>与 {@link #createStatement()} 的关系：{@code executeBatch} 保持标准
     * JDBC 语义（SQL 通道 + 多行 VALUES 合并），适合中小批量；本方法面向
     * 百万行级装载——绕过 SQL 文本解析，内核实测快一个量级。
     *
     * @param table 目标表名（列序/类型自动取自表 schema，含分区列）
     */
    public QsdbBulkWriter bulkWriter(String table) throws SQLException {
        checkOpen();
        return new QsdbBulkWriter(this, table, QsdbBulkWriter.DEFAULT_CHUNK_ROWS);
    }

    /**
     * 创建批量写入器并指定分块行数（内存 = 分块行数 × 列数，越小越省内存、
     * 请求越多）。
     */
    public QsdbBulkWriter bulkWriter(String table, int chunkRows) throws SQLException {
        checkOpen();
        return new QsdbBulkWriter(this, table, chunkRows);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw Errors.unsupported("stored procedures are not supported by QSDB");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw Errors.unsupported("stored procedures are not supported by QSDB");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw Errors.unsupported("stored procedures are not supported by QSDB");
    }

    private static void checkCursorShape(int type, int concurrency, int holdability) throws SQLException {
        if (type != java.sql.ResultSet.TYPE_FORWARD_ONLY) {
            throw Errors.unsupported("only TYPE_FORWARD_ONLY result sets are supported (JSON one-shot responses)");
        }
        if (concurrency != java.sql.ResultSet.CONCUR_READ_ONLY) {
            throw Errors.unsupported("only CONCUR_READ_ONLY result sets are supported (v1 read-only)");
        }
        if (holdability != java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw Errors.unsupported("only java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT holdability is supported");
        }
    }

    // ------------------------------------------------------------------
    // 事务语义（§九）
    // ------------------------------------------------------------------

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        if (!autoCommit) {
            // 明确拒绝而非假成功；连接池配置 autoCommit=true 即完全兼容
            throw Errors.unsupported("QSDB is a read-only analytical store without transactions;"
                    + " setAutoCommit(false) is refused (configure pool autoCommit=true)");
        }
        // setAutoCommit(true)：幂等 no-op
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        checkOpen();
        throw new SQLException("commit() is not applicable: QSDB is read-only with autoCommit always true"
                + " (no transaction model)", "0A000");
    }

    @Override
    public void rollback() throws SQLException {
        checkOpen();
        throw new SQLException("rollback() is not applicable: QSDB is read-only with autoCommit always true"
                + " (no transaction model)", "0A000");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        // 快照读语义下 READ_COMMITTED 与 NONE 等价——接受（§九）
        if (level != TRANSACTION_READ_COMMITTED && level != TRANSACTION_NONE) {
            throw Errors.invalid("transaction isolation " + level
                    + " is not supported; QSDB accepts READ_COMMITTED or NONE only");
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return TRANSACTION_READ_COMMITTED; // 快照隔离的最近似报告
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw Errors.unsupported("savepoints are not supported (no transaction model)");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw Errors.unsupported("savepoints are not supported (no transaction model)");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw Errors.unsupported("savepoints are not supported (no transaction model)");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw Errors.unsupported("savepoints are not supported (no transaction model)");
    }

    // ------------------------------------------------------------------
    // 连接属性
    // ------------------------------------------------------------------

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        // 接受并如实报告（2026-10-13 驱动放开 SQL 写路径后不再恒只读，§九）。
        // 本标志是给优化器/工具的提示，不改变驱动能力面
        // （server 侧 --read-only 与角色权限才是强制边界）
        this.readOnlyHint = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return readOnlyHint;
    }

    /**
     * 设置默认 catalog = 目标库（P2 多库：JDBC catalog 映射 QSDB database——
     * MySQL 生态同款惯例；名字进 SQL 限定符 {@code app.t} 与 body
     * {@code database}）。名字按内核挂载名规则校验（小写
     * {@code [a-z_][a-z0-9_]*}）；存在性由服务端在查询时裁决（未挂载 404）。
     * null/空 = 清除设置（回落连接属性 {@code database}）。
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        if (catalog == null || catalog.isEmpty()) {
            currentCatalog = null;
            return;
        }
        if (!catalog.matches("[a-z_][a-z0-9_]*")) {
            throw Errors.invalid("catalog must be a mounted database name"
                    + " (lowercase [a-z_][a-z0-9_]*), got '" + catalog + "'");
        }
        currentCatalog = catalog;
        invalidateMetadataCache(); // 表集随库而变，缓存按库失效最诚实
    }

    /** 当前 catalog（{@code setCatalog} 显式值；未设置返回 null——规范允许）。 */
    @Override
    public String getCatalog() throws SQLException {
        checkOpen();
        return currentCatalog;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkOpen();
        // 单库：内核 DEFAULT_DB "default" 之外的 schema 拒绝
        if (schema != null && !schema.isEmpty() && !"default".equalsIgnoreCase(schema)) {
            throw Errors.invalid("schema '" + schema + "' does not exist (QSDB is single-database;"
                    + " the only schema is 'default')");
        }
    }

    @Override
    public String getSchema() throws SQLException {
        checkOpen();
        return "default";
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkOpen();
        if (holdability != java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw Errors.unsupported("only java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT holdability is supported");
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkOpen();
        return 0; // 未设置应用层网络超时
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkOpen();
        throw Errors.unsupported("setNetworkTimeout is not supported"
                + " (per-query timeouts are set via Statement.setQueryTimeout)");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close(); // 无阻塞操作需要强制中断：语义等价 close
    }

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------

    @Override
    public void close() throws SQLException {
        if (closed) {
            return; // 幂等——连接池借还兼容性要求（§十一）
        }
        closed = true;
        List<QsdbStatement> toClose = new ArrayList<QsdbStatement>(statements);
        SQLException first = null;
        for (QsdbStatement stmt : toClose) {
            try {
                stmt.close();
            } catch (SQLException e) {
                first = first == null ? e : first;
            }
        }
        statements.clear();
        cachedTables = null;
        cachedSchemas.clear();
        allSchemas = null;
        transport.close();
        if (first != null) {
            throw first;
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // JDBC 规范语义：网络异常返回 false（不抛）；超时上限秒级换算
        if (closed) {
            return false;
        }
        long ms = timeout > 0 ? timeout * 1000L : 0L;
        return transport.healthy(ms);
    }

    // ------------------------------------------------------------------
    // 元数据与杂项
    // ------------------------------------------------------------------

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        if (metadata == null) {
            metadata = new QsdbDatabaseMetaData(this);
        }
        return metadata;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        return sql; // 文本 SQL 直传，无转义改写（v1 非目标）
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkOpen();
        if (map != null && !map.isEmpty()) {
            throw Errors.unsupported("custom type maps are not supported");
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkOpen();
        return new HashMap<String, Class<?>>();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkOpen();
        return new Properties();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // 未支持的属性按 REASON_UNKNOWN 拒绝——不静默吞掉
        if (properties != null && !properties.isEmpty()) {
            Map<String, ClientInfoStatus> failed = new HashMap<String, ClientInfoStatus>();
            for (Object k : properties.keySet()) {
                failed.put(String.valueOf(k), ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            }
            throw new SQLClientInfoException("client info properties are not supported", failed);
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        Map<String, ClientInfoStatus> failed = new HashMap<String, ClientInfoStatus>();
        failed.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
        throw new SQLClientInfoException("client info properties are not supported", failed);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw Errors.unsupported("LOB locators are not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw Errors.unsupported("LOB locators are not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw Errors.unsupported("LOB locators are not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw Errors.unsupported("SQLXML is not supported");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw Errors.unsupported("ARRAY parameters are not supported (v1 read-only)");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw Errors.unsupported("STRUCT parameters are not supported (v1 read-only)");
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

    /** 脱敏展示：只含 host:port，绝无 token（评审红线 §十二）。 */
    @Override
    public String toString() {
        return "QsdbConnection(" + url + (closed ? ", closed" : "") + ")";
    }
}
