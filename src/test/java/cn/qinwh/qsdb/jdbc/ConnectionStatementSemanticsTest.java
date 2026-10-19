package cn.qinwh.qsdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import cn.qinwh.qsdb.jdbc.transport.QueryResponse;

/**
 * 连接/语句语义单测（§九 事务、§十五 非目标）——不触网，
 * connect 阶段无网络 I/O（§四.2）。
 */
class ConnectionStatementSemanticsTest {

    private static Connection connect() throws SQLException {
        // 不可达地址无妨：connect 不建网络连接；本测试全部不触网
        return DriverManager.getConnection("jdbc:qsdb://192.0.2.1:1");
    }

    // ---- §九 事务语义 ----

    @Test
    void transactionSemantics() throws SQLException {
        Connection c = connect();
        assertTrue(c.getAutoCommit());
        c.setAutoCommit(true); // no-op 幂等
        SQLFeatureNotSupportedException e =
                assertThrows(SQLFeatureNotSupportedException.class, () -> c.setAutoCommit(false));
        assertEquals("0A000", e.getSQLState());
        SQLException ce = assertThrows(SQLException.class, c::commit);
        assertEquals("0A000", ce.getSQLState());
        SQLException re = assertThrows(SQLException.class, c::rollback);
        assertEquals("0A000", re.getSQLState());

        // 隔离级别：接受 READ_COMMITTED / NONE，其他拒绝
        c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        c.setTransactionIsolation(Connection.TRANSACTION_NONE);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, c.getTransactionIsolation());
        SQLException badIso = assertThrows(SQLException.class,
                () -> c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));

        // 只读提示：默认可写，setReadOnly 如实报告（2026-10-13 能力面对齐后）
        assertFalse(c.isReadOnly());
        c.setReadOnly(true);
        assertTrue(c.isReadOnly());
        c.setReadOnly(false);
        c.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> c.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
        assertEquals("default", c.getSchema());
        assertNull(c.getCatalog());
        assertThrows(SQLException.class, () -> c.setSchema("other"));
        c.setSchema("default"); // 接受
        assertThrows(SQLFeatureNotSupportedException.class, c::setSavepoint);
    }

    // ---- 游标形态约束 ----

    @Test
    void cursorShapeEnforced() {
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> connect().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> connect().createStatement(ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_UPDATABLE));
        assertThrows(SQLFeatureNotSupportedException.class, () -> connect().prepareCall("{call x}"));
    }

    // ---- 语句属性与回执语义（不触网部分）----

    @Test
    void statementPropertyValidation() throws SQLException {
        Connection c = connect();
        Statement s = c.createStatement();
        s.setQueryTimeout(30);
        assertEquals(30, s.getQueryTimeout());
        assertThrows(SQLException.class, () -> s.setQueryTimeout(-1));
        s.setMaxRows(100);
        assertEquals(100, s.getMaxRows());
        assertThrows(SQLException.class, () -> s.setMaxRows(-1));
        s.setFetchSize(50); // P3 起真流式（>0 = 流式通道；0 = 一次性）
        assertEquals(50, s.getFetchSize());
        s.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertEquals(-1, s.getUpdateCount()); // 未执行
        assertThrows(SQLFeatureNotSupportedException.class, s::getGeneratedKeys);
        s.cancel(); // 无执行中语句 → no-op（J4 后实装为真实取消，见 IT）
        s.addBatch("SELECT 1"); // 批量已支持（§十五：逐条执行实现）
        s.clearBatch();
        assertThrows(SQLFeatureNotSupportedException.class, () -> s.executeUpdate("SELECT 1",
                Statement.RETURN_GENERATED_KEYS));
        s.close();
        assertTrue(s.isClosed());
        assertThrows(SQLException.class, () -> s.executeQuery("SELECT 1"));
    }

    @Test
    void preparedStatementBasics() throws SQLException {
        Connection c = connect();
        // 占位符计数在构造期完成：个数不匹配 fail-fast 不需要网络
        PreparedStatement ps = c.prepareStatement("SELECT ?, ?, 'x?'");
        assertEquals(2, ps.getParameterMetaData().getParameterCount());
        ps.setInt(1, 42);
        ps.setString(2, "s");
        assertEquals(java.sql.Types.BIGINT, ps.getParameterMetaData().getParameterType(1));
        assertEquals(java.sql.Types.VARCHAR, ps.getParameterMetaData().getParameterType(2));
        // 未设置的参数 → clearParameters 后重设
        ps.clearParameters();
        assertThrows(SQLException.class, () -> ps.getMetaData()); // 执行前不可得（诚实）
        ps.setNull(1, java.sql.Types.BIGINT);
        ps.setString(2, "x");
        // 参数索引越界
        assertThrows(SQLException.class, () -> ps.setString(3, "out of range"));
        assertThrows(SQLException.class, () -> ps.setString(0, "zero"));
        // setBytes 自 2026-10-19 P1 转正（server 通道 hex / text 通道 X'…'），
        // 不再是拒绝形态；BINARY 目标类型在 set 期接受
        ps.setBytes(1, new byte[] {0x61});
        assertThrows(SQLException.class, () -> ps.setBytes(0, new byte[1])); // 越界仍拒
        ps.addBatch(); // 参数快照批量已支持（未设参数在 addBatch 时校验）
        ps.clearBatch();
        ps.close();
        assertTrue(ps.isClosed());
        c.close();
    }

    @Test
    void rowLevelUpdateRejectedEngineAligned() throws Exception {
        // 2026-10-13 能力面对齐引擎（§十五）：仅拦截存储模型级不支持的
        // 行级更新族（UPDATE/MERGE/REPLACE/UPSERT）；INSERT/分区 DELETE/DDL
        // 放行（对真内核的放行验证见 IT）
        assertEquals("INSERT", QsdbStatement.firstKeyword("/* c */ -- x\n  insert INTO t VALUES(1)"));
        assertEquals("SELECT", QsdbStatement.firstKeyword("SELECT 1"));
        assertEquals("WITH", QsdbStatement.firstKeyword("  WITH a AS (SELECT 1) SELECT * FROM a"));
        assertNullSafe(QsdbStatement.firstKeyword("(SELECT 1)"));
        java.lang.reflect.Method m = QsdbStatement.class.getDeclaredMethod("rejectDml",
                String.class, String.class);
        m.setAccessible(true);
        // 行级更新族 → SQLFeatureNotSupportedException
        for (String sql : new String[] {"update t set a=1", "-- c\nMERGE INTO t",
                "/* x */ replace into t", "UPSERT INTO t"}) {
            try {
                m.invoke(null, sql, "execute");
                throw new AssertionError("expected rejection: " + sql);
            } catch (java.lang.reflect.InvocationTargetException expected) {
                assertTrue(expected.getCause() instanceof SQLFeatureNotSupportedException, sql);
            }
        }
        // 引擎支持的语句放行（不抛）
        m.invoke(null, "SELECT 1", "execute");
        m.invoke(null, "INSERT INTO t VALUES (1)", "execute");
        m.invoke(null, "  DELETE FROM t WHERE dt = DATE '2026-01-01'", "execute");
        m.invoke(null, "CREATE TABLE t (a INT)", "execute");
        m.invoke(null, "DROP TABLE t", "execute");
        m.invoke(null, "SHOW TABLES", "execute");
        m.invoke(null, "EXPLAIN SELECT 1", "execute");
        m.invoke(null, "WITH a AS (SELECT 1) SELECT * FROM a", "execute");
    }

    @Test
    void affectedRowsParsing() {
        // 内核 DML 回执文案解析（J12 结构化字段落地前的权宜，实测形态）
        assertEquals(1, QsdbStatement.parseAffectedRows("1 rows inserted"));
        assertEquals(25, QsdbStatement.parseAffectedRows("25 rows inserted"));
        assertEquals(2, QsdbStatement.parseAffectedRows("2 rows deleted"));
        assertEquals(7, QsdbStatement.parseAffectedRows("7 rows truncated"));
        assertEquals(0, QsdbStatement.parseAffectedRows("table created"));
        assertEquals(0, QsdbStatement.parseAffectedRows("column 'x' added"));
        assertEquals(0, QsdbStatement.parseAffectedRows(null));
    }

    @Test
    void affectedRowsPrefersStructuredFieldOverMessage() {
        // J12 结构化字段优先；message 文案仅服务旧 server 回退——两者并存时
        // 必须信结构化字段（文案不做兼容承诺，且 executeBatch 合并路径曾因
        // 只读文案与单条路径不一致，2026-10-19 对齐）
        assertEquals(3L, QsdbStatement.affectedRows(
                QueryResponse.ofMessage("ignored message", null, null, "INSERT", 3L)));
        // 结构化字段缺失（旧 server / DDL）→ 文案回退
        assertEquals(7L, QsdbStatement.affectedRows(
                QueryResponse.ofMessage("7 rows truncated", null, null, null, null)));
        // DDL：结构化与文案都无行数 → 0（"没有行数概念"对 executeUpdate 恰为 0）
        assertEquals(0L, QsdbStatement.affectedRows(
                QueryResponse.ofMessage("table created", null, null, null, null)));
    }

    private static void assertNullSafe(String s) {
        // "(SELECT..." 首字符非字母 → null
        if (s != null) {
            assertEquals("SELECT", s); // 容忍未来增强：跳过括号后仍能识别
        }
    }
}
