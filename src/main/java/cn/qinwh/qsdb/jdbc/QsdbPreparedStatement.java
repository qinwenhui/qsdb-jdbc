package cn.qinwh.qsdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import cn.qinwh.qsdb.jdbc.internal.BatchMerge;
import cn.qinwh.qsdb.jdbc.internal.Errors;
import cn.qinwh.qsdb.jdbc.internal.ParamJson;
import cn.qinwh.qsdb.jdbc.internal.ParamRenderer;
import cn.qinwh.qsdb.jdbc.transport.QueryResponse;
import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * 预处理语句（设计文档 §七；2026-10-19 P1 起双通道）。
 *
 * <p><b>server 通道（默认，{@code paramStyle=server}）</b>：{@code ?} 原样
 * 随 SQL 上线（内核解析前经 sqlparser 固化成 {@code $n}），值经 body
 * {@code params} 进<b>内核绑定器</b>转 AST 字面量——**值从不进入 SQL 文本**，
 * 注入防护由内核保证；BINARY / NaN / 控制字符字符串等文本通道拒绝的形态
 * 全部可用（{@link ParamJson} 编码契约）。
 *
 * <p><b>text 通道（{@code paramStyle=text}，服务旧内核的回退）</b>：
 * {@code ?} 在驱动侧渲染为类型化 SQL 字面量（clickhouse-jdbc / trino-jdbc
 * 同款路线）——{@code internal.ParamRenderer} 是唯一生成 SQL 的地方，字符串
 * 参数 {@code ''} 转义、控制字符/NaN 拒绝、BINARY 走 {@code X'…'} 字面量。
 *
 * <p>参数个数在构造期由占位符扫描确定；个数不符/未设参数在执行前
 * fail-fast（内核侧同一判据再查一遍，文本计数与 tokenizer 一致）。
 */
class QsdbPreparedStatement extends QsdbStatement implements PreparedStatement {

    private final String sql;
    private final ParamRenderer.Param[] params;
    private final List<ParamRenderer.Param[]> batch = new ArrayList<ParamRenderer.Param[]>();

    QsdbPreparedStatement(QsdbConnection connection, String sql) throws SQLException {
        super(connection);
        if (sql == null) {
            throw Errors.invalid("SQL is null");
        }
        this.sql = sql;
        this.params = new ParamRenderer.Param[ParamRenderer.countPlaceholders(sql)];
    }

    // ------------------------------------------------------------------
    // 执行（按 paramStyle 解析后走基类统一通道）
    // ------------------------------------------------------------------

    @Override
    public java.sql.ResultSet executeQuery() throws SQLException {
        ensureOpen();
        rejectDml(sql, "executeQuery");
        QueryResponse resp = resolveAndExecute();
        if (resp == null) {
            return attachStreamedResultSet(); // 流式 SELECT（P3）
        }
        if (!resp.isQuery()) {
            throw Errors.invalid("statement did not produce a result set (message: "
                    + resp.getMessage() + "); use execute() for non-query SQL");
        }
        return attachResultSet(resp);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (int) Math.min(executeLargeUpdate(), Integer.MAX_VALUE);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        ensureOpen();
        rejectDml(sql, "executeUpdate");
        QueryResponse resp = resolveAndExecute();
        if (resp != null && resp.isQuery()) {
            throw Errors.invalid("statement produces a result set; use executeQuery()");
        }
        if (resp == null) {
            // fetchSize>0 且语句是 SELECT——executeUpdate 语义错位，规范报错
            throw Errors.invalid("statement produces a result set; use executeQuery()");
        }
        return affectedRows(resp); // J12：结构化 rowsAffected 优先
    }

    @Override
    public boolean execute() throws SQLException {
        ensureOpen();
        rejectDml(sql, "execute");
        QueryResponse resp = resolveAndExecute();
        if (resp == null) {
            attachStreamedResultSet(); // 流式 SELECT（P3）
            return true;
        }
        if (resp.isQuery()) {
            attachResultSet(resp);
            return true;
        }
        return false;
    }

    /**
     * 按 {@code paramStyle} 解析参数并执行（fetchSize&gt;0 时流式优先，见基类
     * {@link QsdbStatement#executeResolved}）：
     * <ul>
     *   <li><b>server</b>（默认）——SQL 原样（{@code ?} 留给内核固化），参数
     *       编码进 body {@code params}（{@link ParamJson}；绑定在内核绑定器）；</li>
     *   <li><b>text</b>——{@link ParamRenderer} 渲染为类型化字面量后下发。</li>
     * </ul>
     * 两通道共享同一套 {@code setXxx} 目标类型与占位符计数，值语义一致——
     * 差分测试见集成用例。
     *
     * @return 一次性结果；流式 SELECT 时 null（结果在 pendingStream）
     */
    private QueryResponse resolveAndExecute() throws SQLException {
        if (connection.serverParams()) {
            ParamRenderer.validate(sql, params); // 个数/未设 fail-fast（内核同判据再查）
            return executeResolved(sql, ParamJson.toJsonArray(params));
        }
        return executeResolved(ParamRenderer.render(sql, params), null);
    }

    /**
     * 把当前参数集快照加入批（渲染在 executeBatch 时统一进行——快照存
     * {@link ParamRenderer.Param}，同一组参数重复利用且免提前渲染失败）。
     */
    @Override
    public void addBatch() throws SQLException {
        ensureOpen();
        ParamRenderer.validate(sql, params);
        batch.add(params.clone());
    }

    @Override
    public void clearBatch() {
        batch.clear();
    }

    /**
     * 执行批（2026-10-13 合并优化）。
     *
     * <p>模板可拆分时（{@code INSERT … VALUES (?…)} 且占位符全在 VALUES 之后），
     * 每份参数快照渲染为元组文本，再折叠成单条多行 VALUES 语句下发（按行数/
     * 字节分块）——消除内核每语句一次的 append+manifest 提交开销。模板形态
     * 不符（占位符跨 VALUES、含尾随子句等）则回退逐条渲染执行，绝不改写。
     *
     * <p>返回 {@code int[]} 仍与原批条目逐项对应。失败即抛，已执行的组各自
     * 生效（内核无事务模型，不回滚）；合并后同组条目无法逐条归因失败行。
     */
    @Override
    public int[] executeBatch() throws SQLException {
        ensureOpen();
        List<ParamRenderer.Param[]> entries = new ArrayList<ParamRenderer.Param[]>(batch);
        batch.clear();
        if (entries.isEmpty()) {
            return new int[0];
        }
        for (ParamRenderer.Param[] snapshot : entries) {
            ParamRenderer.validate(sql, snapshot); // fail-fast：未设参数/个数不符
        }
        int[] results = new int[entries.size()];

        BatchMerge.Template tpl = connection.batchRewrite()
                ? BatchMerge.splitTemplate(sql, params.length)
                : null; // 逃生舱（batchRewrite=false）：跳过分合并，直接走逐条
        if (tpl == null) {
            // 形态不符或已禁用合并 → 逐条渲染执行（诚实回退，不如猜测）
            for (int i = 0; i < entries.size(); i++) {
                results[i] = executeUpdateWith(entries.get(i));
            }
            return results;
        }
        // 按窗口渲染 + 合并（窗口 = 组行数上限，组不可能跨窗口；峰值分配
        // 从 O(批) 降到 O(窗口)，百万级批次友好）
        int window = BatchMerge.MAX_ROWS_PER_STATEMENT;
        for (int base = 0; base < entries.size(); base += window) {
            int end = Math.min(base + window, entries.size());
            List<BatchMerge.Fragment> fragments = new ArrayList<BatchMerge.Fragment>(end - base);
            boolean mergeable = true;
            for (int i = base; i < end; i++) {
                String tuple;
                try {
                    tuple = ParamRenderer.render(tpl.tupleTemplate, entries.get(i));
                } catch (SQLException unrenderable) {
                    // 参数含文本通道不可渲染形态（NaN 等）——本窗口放弃合并，
                    // 逐条走 executeUpdateWith（server 风格经 params 通道，
                    // 此类参数可执行；text 风格沿用渲染语义）
                    mergeable = false;
                    break;
                }
                BatchMerge.Fragment f = BatchMerge.fragmentOfRenderedTuple(tpl.prefix, tuple);
                if (!f.isMergeable()) {
                    mergeable = false;
                    break;
                }
                fragments.add(f);
            }
            if (!mergeable) {
                for (int i = base; i < end; i++) {
                    results[i] = executeUpdateWith(entries.get(i));
                }
                continue;
            }
            for (BatchMerge.Group g : BatchMerge.group(fragments)) {
                int affected = executeUpdateRaw(g.sql);
                if (g.counts != null) {
                    System.arraycopy(g.counts, 0, results, base + g.entryStart, g.entryCount);
                } else {
                    results[base + g.entryStart] = affected;
                }
            }
        }
        return results;
    }

    /** 用给定参数执行（不扰动当前参数集；executeBatch 回退路径）。 */
    private int executeUpdateWith(ParamRenderer.Param[] snapshot) throws SQLException {
        rejectDml(sql, "executeUpdate");
        QueryResponse resp;
        if (connection.serverParams()) {
            ParamRenderer.validate(sql, snapshot);
            resp = executeInternal(sql, ParamJson.toJsonArray(snapshot));
        } else {
            resp = executeInternal(ParamRenderer.render(sql, snapshot));
        }
        if (resp.isQuery()) {
            throw Errors.invalid("statement produces a result set; use executeQuery()");
        }
        // 与单条路径同一受影响行数语义：结构化 rowsAffected 优先（J12），
        // 旧 server 回退解析 message 文案（此前这里只走文案解析，两路不一致）
        return (int) Math.min(QsdbStatement.affectedRows(resp), Integer.MAX_VALUE);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureOpen();
        if (lastColumns == null) {
            throw Errors.invalid("result metadata is unavailable before execution"
                    + " (QSDB has no server-side prepare; run the statement once or use"
                    + " DatabaseMetaData.getColumns for table column metadata)");
        }
        return QsdbResultSetMetaData.of(lastColumns);
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return new QsdbParameterMetaData();
    }

    // ------------------------------------------------------------------
    // setXxx（§6.2 渲染目标类型表）
    // ------------------------------------------------------------------

    private void set(int idx, QsdbType type, Object value) throws SQLException {
        checkIndex(idx);
        params[idx - 1] = ParamRenderer.Param.of(type, value);
    }

    private void checkIndex(int idx) throws SQLException {
        if (idx < 1 || idx > params.length) {
            throw Errors.invalid("parameter index " + idx + " out of range 1.." + params.length
                    + " (" + params.length + " placeholder(s) in SQL)");
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        set(parameterIndex, QsdbType.BIGINT, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        set(parameterIndex, QsdbType.BIGINT, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        set(parameterIndex, QsdbType.BIGINT, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        set(parameterIndex, QsdbType.BIGINT, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        set(parameterIndex, QsdbType.FLOAT, x); // NaN/Infinity 在渲染期拒绝（§6.2）
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        set(parameterIndex, QsdbType.DOUBLE, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        set(parameterIndex, QsdbType.DECIMAL, x);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        set(parameterIndex, QsdbType.BOOLEAN, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        set(parameterIndex, QsdbType.VARCHAR, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        set(parameterIndex, QsdbType.DATE, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        // QSDB DATE 无时区语义；cal 忽略（文档化的宽容实现）
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        set(parameterIndex, QsdbType.TIME, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        set(parameterIndex, QsdbType.TIMESTAMP, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkIndex(parameterIndex);
        params[parameterIndex - 1] = ParamRenderer.Param.nullValue(QsdbType.forJdbcType(sqlType));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.VARCHAR);
            return;
        }
        // §6.2：按入参 Java 类型走上行映射
        if (x instanceof Byte || x instanceof Short || x instanceof Integer || x instanceof Long) {
            set(parameterIndex, QsdbType.BIGINT, x);
        } else if (x instanceof Float) {
            set(parameterIndex, QsdbType.FLOAT, x);
        } else if (x instanceof Double) {
            set(parameterIndex, QsdbType.DOUBLE, x);
        } else if (x instanceof BigDecimal) {
            set(parameterIndex, QsdbType.DECIMAL, x);
        } else if (x instanceof Boolean) {
            set(parameterIndex, QsdbType.BOOLEAN, x);
        } else if (x instanceof String) {
            set(parameterIndex, QsdbType.VARCHAR, x);
        } else if (x instanceof Date) {
            set(parameterIndex, QsdbType.DATE, x);
        } else if (x instanceof Time) {
            set(parameterIndex, QsdbType.TIME, x);
        } else if (x instanceof Timestamp) {
            set(parameterIndex, QsdbType.TIMESTAMP, x);
        } else if (x instanceof java.time.LocalDate) {
            set(parameterIndex, QsdbType.DATE, Date.valueOf((java.time.LocalDate) x));
        } else if (x instanceof java.time.LocalTime) {
            // 直接存 LocalTime（Time.valueOf 会**静默丢纳秒**——Time 是秒精度的
            // java.util.Date 薄壳，toLocalTime() 归零小数秒）；编码层原生接受
            set(parameterIndex, QsdbType.TIME, x);
        } else if (x instanceof java.time.LocalDateTime) {
            set(parameterIndex, QsdbType.TIMESTAMP, Timestamp.valueOf((java.time.LocalDateTime) x));
        } else {
            throw Errors.unsupported("setObject with " + x.getClass().getName()
                    + " is not supported (v1 read-only)");
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber() == null
                ? Types.VARCHAR : targetSqlType.getVendorTypeNumber());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
            return;
        }
        QsdbType target = QsdbType.forJdbcType(targetSqlType);
        switch (target) {
            case BIGINT:
                if (x instanceof Number) {
                    set(parameterIndex, target, ((Number) x).longValue());
                    return;
                }
                if (x instanceof String) {
                    set(parameterIndex, target, Long.parseLong((String) x));
                    return;
                }
                break;
            case FLOAT:
                if (x instanceof Number) {
                    set(parameterIndex, target, ((Number) x).floatValue());
                    return;
                }
                if (x instanceof String) {
                    set(parameterIndex, target, Float.parseFloat((String) x));
                    return;
                }
                break;
            case DOUBLE:
                if (x instanceof Number) {
                    set(parameterIndex, target, ((Number) x).doubleValue());
                    return;
                }
                if (x instanceof String) {
                    set(parameterIndex, target, Double.parseDouble((String) x));
                    return;
                }
                break;
            case DECIMAL:
                if (x instanceof BigDecimal) {
                    set(parameterIndex, target, x);
                    return;
                }
                if (x instanceof Number) {
                    set(parameterIndex, target, new BigDecimal(x.toString()));
                    return;
                }
                if (x instanceof String) {
                    set(parameterIndex, target, new BigDecimal((String) x));
                    return;
                }
                break;
            case BOOLEAN:
                if (x instanceof Boolean) {
                    set(parameterIndex, target, x);
                    return;
                }
                if (x instanceof String) {
                    set(parameterIndex, target, Boolean.parseBoolean((String) x));
                    return;
                }
                if (x instanceof Number) {
                    set(parameterIndex, target, ((Number) x).doubleValue() != 0d);
                    return;
                }
                break;
            case VARCHAR:
            case TEXT:
                set(parameterIndex, target, String.valueOf(x));
                return;
            case DATE:
                if (x instanceof Date) {
                    set(parameterIndex, target, x);
                    return;
                }
                if (x instanceof String) {
                    set(parameterIndex, target, Date.valueOf((String) x));
                    return;
                }
                if (x instanceof java.time.LocalDate) {
                    set(parameterIndex, target, Date.valueOf((java.time.LocalDate) x));
                    return;
                }
                break;
            case TIME:
                if (x instanceof Time) {
                    set(parameterIndex, target, x);
                    return;
                }
                if (x instanceof String) {
                    set(parameterIndex, target, Time.valueOf((String) x));
                    return;
                }
                if (x instanceof java.time.LocalTime) {
                    set(parameterIndex, target, Time.valueOf((java.time.LocalTime) x));
                    return;
                }
                break;
            case TIMESTAMP:
                if (x instanceof Timestamp) {
                    set(parameterIndex, target, x);
                    return;
                }
                if (x instanceof String) {
                    set(parameterIndex, target, Timestamp.valueOf((String) x));
                    return;
                }
                if (x instanceof java.time.LocalDateTime) {
                    set(parameterIndex, target, Timestamp.valueOf((java.time.LocalDateTime) x));
                    return;
                }
                break;
            default:
                break;
        }
        throw Errors.invalid("cannot coerce " + x.getClass().getName() + " to JDBC type " + targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber() == null
                ? Types.VARCHAR : targetSqlType.getVendorTypeNumber(), scaleOrLength);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        set(parameterIndex, QsdbType.VARCHAR, x == null ? null : x.toExternalForm());
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw Errors.unsupported("stream parameters are not supported (v1 read-only)");
    }

    /**
     * BINARY 参数（2026-10-19 P1 转正）：server 通道经
     * {@code {"type":"BINARY","value":"hex"}} 进内核绑定器；text 通道渲染
     * {@code X'…'} 字面量（内核第九轮起支持）。防御性拷贝——快照语义与
     * {@code addBatch} 一致。
     */
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        set(parameterIndex, QsdbType.BINARY, x == null ? null : x.clone());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw Errors.unsupported("binary stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw Errors.unsupported("binary stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw Errors.unsupported("binary stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw Errors.unsupported("stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw Errors.unsupported("stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw Errors.unsupported("stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw Errors.unsupported("stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw Errors.unsupported("stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw Errors.unsupported("stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw Errors.unsupported("stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw Errors.unsupported("stream parameters are not supported (v1 read-only)");
    }

    @Override
    public void setBlob(int parameterIndex, java.sql.Blob x) throws SQLException {
        throw Errors.unsupported("setBlob is not supported (v1 read-only)");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw Errors.unsupported("setBlob is not supported (v1 read-only)");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw Errors.unsupported("setBlob is not supported (v1 read-only)");
    }

    @Override
    public void setClob(int parameterIndex, java.sql.Clob x) throws SQLException {
        throw Errors.unsupported("setClob is not supported (v1 read-only)");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw Errors.unsupported("setClob is not supported (v1 read-only)");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw Errors.unsupported("setClob is not supported (v1 read-only)");
    }

    @Override
    public void setNClob(int parameterIndex, java.sql.NClob value) throws SQLException {
        throw Errors.unsupported("setNClob is not supported (v1 read-only)");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw Errors.unsupported("setNClob is not supported (v1 read-only)");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw Errors.unsupported("setNClob is not supported (v1 read-only)");
    }

    @Override
    public void setArray(int parameterIndex, java.sql.Array x) throws SQLException {
        throw Errors.unsupported("setArray is not supported (v1 read-only)");
    }

    @Override
    public void setRef(int parameterIndex, java.sql.Ref x) throws SQLException {
        throw Errors.unsupported("setRef is not supported (v1 read-only)");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw Errors.unsupported("RowId is not supported (no row-level identity)");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw Errors.unsupported("SQLXML is not supported");
    }

    @Override
    public void clearParameters() {
        java.util.Arrays.fill(params, null);
    }

    // ------------------------------------------------------------------
    // 参数元数据（最小实现）
    // ------------------------------------------------------------------

    /**
     * 最小 {@link ParameterMetaData}：参数个数来自占位符扫描；已设置参数
     * 报告其渲染目标类型，未设置参数如实报告 VARCHAR 兜底（无服务端
     * 预编译协议，无法先知声明类型）。
     */
    private final class QsdbParameterMetaData implements ParameterMetaData {

        private QsdbType typeOf(int param) {
            int idx = param - 1;
            if (idx < 0 || idx >= params.length || params[idx] == null) {
                return QsdbType.VARCHAR;
            }
            return params[idx].targetType;
        }

        private void check(int param) throws SQLException {
            if (param < 1 || param > params.length) {
                throw Errors.invalid("parameter index " + param + " out of range 1.." + params.length);
            }
        }

        @Override
        public int getParameterCount() {
            return params.length;
        }

        @Override
        public int isNullable(int param) throws SQLException {
            check(param);
            return parameterNullableUnknown;
        }

        @Override
        public boolean isSigned(int param) throws SQLException {
            check(param);
            return typeOf(param).isNumeric();
        }

        @Override
        public int getPrecision(int param) throws SQLException {
            check(param);
            return typeOf(param).getDefaultPrecision();
        }

        @Override
        public int getScale(int param) throws SQLException {
            check(param);
            return 0;
        }

        @Override
        public int getParameterType(int param) throws SQLException {
            check(param);
            return typeOf(param).getJdbcType();
        }

        @Override
        public String getParameterTypeName(int param) throws SQLException {
            check(param);
            return typeOf(param).getJdbcTypeName();
        }

        @Override
        public String getParameterClassName(int param) throws SQLException {
            check(param);
            return typeOf(param).getObjectClass().getName();
        }

        @Override
        public int getParameterMode(int param) throws SQLException {
            check(param);
            return parameterModeIn;
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
}
