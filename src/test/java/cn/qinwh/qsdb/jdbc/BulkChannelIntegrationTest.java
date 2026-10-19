package cn.qinwh.qsdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * {@code format=rows} 批量通道的端到端测试（对真 server）。
 *
 * <p><b>门控</b>：除 {@code QSDB_IT_ADDR}/{@code QSDB_IT_TOKEN} 外，还需
 * <b>{@code QSDB_IT_BULK=1}</b> 显式开启——该通道需内核侧 {@code /write}
 * 支持 {@code format=rows}（{@code docs/rows-format.md}）。内核就绪前这些
 * 用例默认跳过，不影响常规回归。
 *
 * <p>编码器本身的正确性<b>不依赖本测试</b>：{@code RowsEncoderTest} 用内核
 * 规范里的 85 字节示例做逐字节一致性锁定，无需联网即可验证。
 */
class BulkChannelIntegrationTest {

    private static String addr;
    private static String token;
    private static String url;

    @BeforeAll
    static void gate() {
        addr = System.getenv("QSDB_IT_ADDR");
        assumeTrue(addr != null && !addr.isEmpty(), "QSDB_IT_ADDR not set — integration tests skipped");
        assumeTrue("1".equals(System.getenv("QSDB_IT_BULK")),
                "QSDB_IT_BULK not set — rows bulk channel tests skipped"
                        + " (needs kernel /write?format=rows support)");
        token = System.getenv("QSDB_IT_TOKEN");
        url = "jdbc:qsdb://" + addr + (token != null ? "?token=" + token : "");
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(url);
    }

    @Test
    void bulkWriteAllTypesRoundTrip() throws SQLException {
        String table = "jdbc_it_bulk";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, name STRING, score DOUBLE,"
                    + " flag BOOLEAN, dec DECIMAL(10,2), dt DATE, ts TIMESTAMP, raw BINARY)");

            int n = 5000;
            QsdbBulkWriter w = QsdbBulkWriter.open(c, table);
            try {
                assertEquals(8, w.getColumnCount());
                for (int i = 0; i < n; i++) {
                    w.setLong("id", i);
                    w.setString("name", "name-" + i);
                    w.setDouble("score", 1.5 + (i % 10));
                    w.setBoolean("flag", i % 2 == 0);
                    w.setBigDecimal("dec", new BigDecimal("12.34"));
                    w.setDate("dt", LocalDate.of(2026, 1, 1).plusDays(i % 7));
                    w.setTimestamp("ts", Timestamp.valueOf("2026-01-01 08:30:00.123456789"));
                    w.setBytes("raw", new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef});
                    if (i % 100 == 7) {
                        w.setNull("name"); // 穿插 NULL（触发位图路径）
                    }
                    w.addRow();
                }
                // 缓冲中尚未提交（默认分块 65536 行 > 5000），故此刻计数为 0
                assertEquals(0, w.getRowCount(), "未 flush 前不计入已提交行数");
            } finally {
                w.close(); // close 提交剩余
            }
            assertEquals(n, w.getRowCount(), "close 后应全部提交");
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(5000L, rs.getLong("n"));
            }
            try (ResultSet rs = s.executeQuery("SELECT * FROM " + table + " WHERE id = 123")) {
                assertTrue(rs.next());
                assertEquals(123L, rs.getLong("id"));
                assertEquals("name-123", rs.getString("name"));
                assertEquals(1.5 + 3, rs.getDouble("score"), 0d);
                assertEquals(false, rs.getBoolean("flag"));
                assertEquals(new BigDecimal("12.34"), rs.getBigDecimal("dec"));
                assertEquals(Date.valueOf("2026-01-05"), rs.getDate("dt")); // 1/1 + (123 % 7 = 4) 天
                assertEquals(Timestamp.valueOf("2026-01-01 08:30:00.123456789"), rs.getTimestamp("ts"));
                assertEquals(4, rs.getBytes("raw").length);
            }
            // NULL 位图路径：id=107 的 name 应为 NULL
            try (ResultSet rs = s.executeQuery("SELECT name FROM " + table + " WHERE id = 107")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
                assertTrue(rs.wasNull());
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void chunkingSplitsIntoMultipleRequests() throws SQLException {
        // 小分块 → 多次请求，结果与一次性写入等价（一次请求一次提交）
        String table = "jdbc_it_bulk_chunk";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT)");
            try (QsdbBulkWriter w = QsdbBulkWriter.open(c, table, 100)) { // 每 100 行一块
                for (int i = 0; i < 1050; i++) {
                    w.setLong(1, i);
                    w.addRow();
                }
            }
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(1050L, rs.getLong("n"));
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void emptyWriterIsNoOp() throws SQLException {
        String table = "jdbc_it_bulk_empty";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT)");
            try (QsdbBulkWriter w = QsdbBulkWriter.open(c, table)) {
                assertEquals(0, w.flush()); // 无行 → 不发请求
                assertEquals(0, w.getRowCount());
            }
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(0L, rs.getLong("n"));
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void typeMismatchRejectedClientSide() throws SQLException {
        // 列类型不符在客户端即拒绝（不把坏数据发出去）
        String table = "jdbc_it_bulk_types";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, name STRING)");
            try (QsdbBulkWriter w = QsdbBulkWriter.open(c, table)) {
                org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                        () -> w.setString("id", "not-an-int"));
                org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                        () -> w.setLong("name", 1L));
                org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                        () -> w.setLong("nope", 1L)); // 列不存在
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void writerIsUsableWithConcurrentConnections() throws SQLException {
        // 并发装载：每连接一个写入器（JDBC 惯例）
        String table = "jdbc_it_bulk_concurrent";
        final int threads = 4;
        final int perThread = 2000;
        Connection setup = connect();
        try {
            Statement s = setup.createStatement();
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, who BIGINT)");
            s.close();
            java.util.concurrent.ExecutorService ex =
                    java.util.concurrent.Executors.newFixedThreadPool(threads);
            java.util.concurrent.CountDownLatch done =
                    new java.util.concurrent.CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                final int who = t;
                ex.submit(() -> {
                    try (Connection c = DriverManager.getConnection(url);
                         QsdbBulkWriter w = QsdbBulkWriter.open(c, table)) {
                        for (int i = 0; i < perThread; i++) {
                            w.setLong("id", (long) who * perThread + i);
                            w.setLong("who", who);
                            w.addRow();
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            try {
                assertTrue(done.await(120, java.util.concurrent.TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new SQLException(e);
            }
            ex.shutdown();
            try (ResultSet rs = setup.createStatement()
                    .executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals((long) threads * perThread, rs.getLong("n"));
            }
        } finally {
            Statement s = setup.createStatement();
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            setup.close();
        }
    }

    @Test
    void bulkWriterOnMissingTableFailsClearly() throws SQLException {
        Connection c = connect();
        try {
            SQLException e = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                    () -> QsdbBulkWriter.open(c, "no_such_table_xyz"));
            assertTrue(e.getMessage().contains("no columns") || e.getMessage().contains("not found"),
                    e.getMessage());
        } finally {
            c.close();
        }
    }
}
