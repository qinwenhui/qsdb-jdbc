package cn.qinwh.qsdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * 集成测试（设计文档 §十三.3 第二层）：对真 server 的全类型回读、
 * 参数化查询、超时、错误码、isValid、元数据。
 *
 * <p>运行条件（环境变量门控，未设跳过——与 qsdb-sync e2e 同风格）：
 * <pre>
 *   QSDB_IT_ADDR   = 127.0.0.1:9430        （server 地址）
 *   QSDB_IT_TOKEN  = &lt;server --token 值&gt;   （可选，开放模式不设）
 * </pre>
 */
class IntegrationTest {

    private static String addr;
    private static String token;
    private static String url;

    @BeforeAll
    static void gate() {
        addr = System.getenv("QSDB_IT_ADDR");
        token = System.getenv("QSDB_IT_TOKEN");
        assumeTrue(addr != null && !addr.isEmpty(), "QSDB_IT_ADDR not set — integration tests skipped");
        url = "jdbc:qsdb://" + addr + (token != null ? "?token=" + token : "");
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(url);
    }

    @Test
    void healthAndIsValid() throws SQLException {
        Connection c = connect();
        assertTrue(c.isValid(5), "isValid via GET /health");
        c.close();
        assertFalse(c.isValid(5)); // closed 连接恒 false
    }

    @Test
    void literalQueryAllTypes() throws SQLException {
        // 全类型面：整数族/浮点/布尔/字符串/DECIMAL/DATE/TIMESTAMP/TIME/BINARY
        // （SELECT 字面量即可覆盖——不依赖库里有业务表）
        // J10 修复后 TIME 字面量恢复；J2 修复后 BINARY 内容可逆 → 强断言
        Connection c = connect();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(
                "SELECT CAST(42 AS TINYINT) AS a_tiny, CAST(300 AS SMALLINT) AS a_small,"
                        + " CAST(70000 AS INT) AS a_int, 123456789012345 AS a_big,"
                        + " CAST(1.5 AS FLOAT) AS a_f32, 2.5 AS a_f64,"
                        + " true AS a_bool, 'hello' AS a_str,"
                        + " CAST('12.34' AS DECIMAL(10,2)) AS a_dec,"
                        + " DATE '2026-10-12' AS a_date,"
                        + " TIMESTAMP '2026-10-12 08:30:00.123' AS a_ts,"
                        + " TIME '08:30:00' AS a_time,"
                        + " CAST('ab' AS BINARY) AS a_bin");
        assertTrue(rs.next());
        assertEquals(42, rs.getByte("a_tiny"));
        assertEquals(300, rs.getShort("a_small"));
        assertEquals(70000, rs.getInt("a_int"));
        assertEquals(123456789012345L, rs.getLong("a_big"));
        assertEquals(1.5f, rs.getFloat("a_f32"), 1e-6f);
        assertEquals(2.5d, rs.getDouble("a_f64"), 0d);
        assertTrue(rs.getBoolean("a_bool"));
        assertEquals("hello", rs.getString("a_str"));
        assertEquals(new BigDecimal("12.34"), rs.getBigDecimal("a_dec"));
        assertEquals(java.sql.Date.valueOf("2026-10-12"), rs.getDate("a_date"));
        assertEquals(java.sql.Timestamp.valueOf("2026-10-12 08:30:00.123"), rs.getTimestamp("a_ts"));
        assertEquals(java.sql.Time.valueOf("08:30:00"), rs.getTime("a_time"));
        // J2 修复：BINARY 恒 hex 字符串，内容可逆 → 强断言（'ab' UTF-8 = 0x61 0x62）
        byte[] bin = rs.getBytes("a_bin");
        assertEquals(2, bin.length);
        assertEquals((byte) 0x61, bin[0]);
        assertEquals((byte) 0x62, bin[1]);
        assertFalse(rs.next());
        rs.close();
        s.close();
        c.close();
    }

    @Test
    void aggregateProjectionContracts() throws SQLException {
        // J13 回归锁定。内核复查时在同一处（count(*) 的 O(1) 元数据短路，
        // 触发条件：无 GROUP BY + 唯一聚合 count(*) + 谓词仅引用分区列）
        // 发现两条**静默错值**——比别名丢失严重得多，故驱动侧从结果契约
        // 角度锁死：列必须都在、值必须算对。
        String table = "jdbc_it_agg";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table
                    + " (id BIGINT, dt DATE) PARTITION BY (dt)");
            assertEquals(3, s.executeUpdate("INSERT INTO " + table
                    + " VALUES (1, DATE '2026-03-01'), (2, DATE '2026-03-01'),"
                    + " (3, DATE '2026-03-02')"));

            // ① 聚合列别名（曾丢失 → 列名透出为 "count(*)"）
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals("n", rs.getMetaData().getColumnLabel(1));
                assertEquals(3L, rs.getLong("n"));
            }
            // ② 聚合 + 普通列并存（曾只回一列，k 被吞）
            try (ResultSet rs = s.executeQuery(
                    "SELECT COUNT(*), 1 AS k FROM " + table)) {
                assertEquals(2, rs.getMetaData().getColumnCount(), "普通列不得被吞");
                assertTrue(rs.next());
                assertEquals(3L, rs.getLong(1));
                assertEquals(1L, rs.getLong("k"));
            }
            // ③ 聚合参与运算（曾把 +1 丢掉，回 3 而非 4）
            try (ResultSet rs = s.executeQuery(
                    "SELECT COUNT(*) + 1 AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(4L, rs.getLong("n"), "聚合的算术运算不得被忽略");
            }
            // ④ 分区谓词走短路路径（触发条件）+ 别名
            try (ResultSet rs = s.executeQuery(
                    "SELECT COUNT(*) AS n FROM " + table + " WHERE dt = DATE '2026-03-01'")) {
                assertTrue(rs.next());
                assertEquals(2L, rs.getLong("n"));
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void dmlReceiptIsStructured() throws SQLException {
        // J12：DML 回执带结构化 command/rowsAffected，驱动不再解析文案；
        // DDL 不出现这两个字段 → executeUpdate 返回 0（"没有行数概念"）
        String table = "jdbc_it_receipt";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            assertEquals(0, s.executeUpdate("CREATE TABLE " + table + " (id BIGINT)"));
            assertEquals(3, s.executeUpdate("INSERT INTO " + table
                    + " VALUES (1), (2), (3)"));
            assertEquals(3L, s.executeLargeUpdate("INSERT INTO " + table
                    + " VALUES (4), (5), (6)"));
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(6L, rs.getLong("n"));
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void jsonAndRowsReadPathsAgree() throws SQLException {
        // **差分测试**：同一批查询分别走 json 与 rows（二进制行式）读回，
        // 逐列逐值必须完全一致。用稳定的 JSON 路径证明新通道，而不是让
        // 使用者凭信心接受（RowsDecoder 的语义正确性由本用例守护）。
        String table = "jdbc_it_diff";
        String urlJson = url + (url.contains("?") ? "&" : "?") + "readFormat=json";
        String urlRows = url + (url.contains("?") ? "&" : "?") + "readFormat=rows";
        try (Connection setup = connect()) {
            Statement s = setup.createStatement();
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, t8 TINYINT, t16 SMALLINT,"
                    + " t32 INT, f32 FLOAT, f64 DOUBLE, flag BOOLEAN, s STRING,"
                    + " dec DECIMAL(12,3), dt DATE, ts TIMESTAMP, tm TIME, bin BINARY)");
            // 用批量通道灌数据：INSERT VALUES 当前不接受 CAST（内核常量表达式
            // 限制），BINARY 列经该路径写入不了；顺便覆盖写侧 NULL 位图
            try (QsdbBulkWriter w = QsdbBulkWriter.open(setup, table)) {
                w.setLong("id", 1); w.setLong("t8", 7); w.setLong("t16", 300);
                w.setLong("t32", 70000); w.setDouble("f32", 1.5); w.setDouble("f64", 2.5);
                w.setBoolean("flag", true); w.setString("s", "hello");
                w.setBigDecimal("dec", new BigDecimal("12.345"));
                w.setDate("dt", java.time.LocalDate.of(2026, 10, 12));
                w.setTimestamp("ts", java.time.LocalDateTime.of(2026, 10, 12, 8, 30, 0, 123456789));
                w.setTime("tm", java.time.LocalTime.of(8, 30, 0));
                w.setBytes("bin", new byte[] {0x61, 0x62});
                w.addRow();

                w.setLong("id", 2); w.setLong("t8", -7); w.setLong("t16", -300);
                w.setLong("t32", -70000); w.setDouble("f32", -1.5); w.setDouble("f64", -2.5);
                w.setBoolean("flag", false); w.setString("s", ""); // 空串 ≠ NULL
                w.setBigDecimal("dec", new BigDecimal("-0.001"));
                w.setDate("dt", java.time.LocalDate.of(1970, 1, 1));
                w.setTimestamp("ts", java.time.LocalDateTime.of(1969, 12, 31, 23, 59, 59));
                w.setTime("tm", java.time.LocalTime.MIDNIGHT);
                w.setBytes("bin", new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef});
                w.addRow();

                w.setLong("id", 3); // 其余列不设 → 全 NULL
                w.addRow();
            }
            s.close();
        }
        // 逐列类型无关地比对（含 NULL 行、空串、负值、边界日期）
        String[] queries = {
                "SELECT id, t8, t16, t32 FROM " + table + " ORDER BY id",
                "SELECT f32, f64, flag FROM " + table + " ORDER BY id",
                "SELECT s, dec FROM " + table + " ORDER BY id",
                "SELECT dt, ts, tm FROM " + table + " ORDER BY id",
                "SELECT bin FROM " + table + " ORDER BY id",
                "SELECT * FROM " + table + " ORDER BY id",
        };
        try (Connection cj = DriverManager.getConnection(urlJson);
             Connection cr = DriverManager.getConnection(urlRows)) {
            for (String q : queries) {
                List<String> viaJson = renderAll(cj, q);
                List<String> viaRows = renderAll(cr, q);
                assertEquals(viaJson, viaRows, "json 与 rows 读路径不一致：" + q);
            }
        } finally {
            try (Connection c = connect()) {
                c.createStatement().execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    /** 把查询结果「列名 + 值形态」渲染成可比较的字符串清单（含 NULL 与 wasNull）。 */
    private static List<String> renderAll(Connection c, String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            StringBuilder head = new StringBuilder("cols:");
            for (int i = 1; i <= md.getColumnCount(); i++) {
                head.append(md.getColumnLabel(i)).append('/')
                        .append(md.getColumnType(i)).append(';');
            }
            out.add(head.toString());
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    Object v = rs.getObject(i);
                    row.append(v == null ? "<NULL>" : canonical(v)).append('|');
                    row.append(rs.wasNull() ? 'N' : '-');
                }
                out.add(row.toString());
            }
        }
        return out;
    }

    /** 值的规范文本（byte[] 十六进制、其余 toString），用于跨格式比对。 */
    private static String canonical(Object v) {
        if (v instanceof byte[]) {
            StringBuilder sb = new StringBuilder();
            for (byte b : (byte[]) v) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }
        return String.valueOf(v);
    }

    @Test
    void pagingAndNullsOrderingSupported() throws SQLException {
        // 内核本批新实现（此前**静默忽略**：OFFSET 被丢掉、NULLS 恒用默认、
        // FETCH 当没写、LIMIT m,n 只取 n）。锁住这几条能力——**BI 工具的
        // 翻页就靠 LIMIT/OFFSET**（DBeaver 等按页拉取）。
        String table = "jdbc_it_paging";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, name STRING)");
            s.executeUpdate("INSERT INTO " + table
                    + " VALUES (1,'b'), (2,NULL), (3,'a'), (4,NULL), (5,'c')");

            assertEquals(Arrays.asList(2L, 3L), longColumn(s,
                    "SELECT id FROM " + table + " ORDER BY id LIMIT 2 OFFSET 1"));
            assertEquals(Arrays.asList(2L, 3L), longColumn(s,
                    "SELECT id FROM " + table + " ORDER BY id LIMIT 1, 2"), "MySQL 形 LIMIT m,n");
            assertEquals(Arrays.asList(4L, 5L), longColumn(s,
                    "SELECT id FROM " + table + " ORDER BY id OFFSET 3"), "不带上界的 OFFSET");
            assertEquals(Arrays.asList(1L, 2L), longColumn(s,
                    "SELECT id FROM " + table + " ORDER BY id FETCH FIRST 2 ROWS ONLY"));

            // NULLS 放置（与 PG 一致）：默认 ASC⇒NULLS LAST、DESC⇒NULLS FIRST
            assertEquals(Arrays.asList(3L, 1L, 5L, 2L, 4L), longColumn(s,
                    "SELECT id FROM " + table + " ORDER BY name ASC NULLS LAST"));
            assertEquals(Arrays.asList(3L, 1L, 5L, 2L, 4L), longColumn(s,
                    "SELECT id FROM " + table + " ORDER BY name ASC"), "默认 ASC ⇒ NULLS LAST");
            assertEquals(Arrays.asList(2L, 4L, 5L, 1L, 3L), longColumn(s,
                    "SELECT id FROM " + table + " ORDER BY name DESC"), "默认 DESC ⇒ NULLS FIRST");

            // 幂等确认：翻页取完 = 全量（BI 翻页的正确性基础）
            List<Long> paged = new ArrayList<Long>();
            for (int off = 0; off < 5; off += 2) {
                paged.addAll(longColumn(s, "SELECT id FROM " + table
                        + " ORDER BY id LIMIT 2 OFFSET " + off));
            }
            assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L), paged, "逐页取完应等于全量");
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    /** 取单列 long 结果（分页/排序断言用）。 */
    private static List<Long> longColumn(Statement s, String sql) throws SQLException {
        List<Long> out = new ArrayList<Long>();
        try (ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                out.add(rs.getLong(1));
            }
        }
        return out;
    }

    @Test
    void aggregateDistinctNativeSupported() throws SQLException {
        // 聚合参数内的 DISTINCT：内核曾**静默按 count(x) 给错数字**（第一轮
        // 行为变更时改为显式拒绝，本用例当时锁定拒绝）——2026-10-19 实测
        // 当前内核已原生实现且回值正确（"超越DuckDB路线" P1⑩ 落地），
        // 用例随之翻转为锁定正确性。SQL 标准的 SELECT DISTINCT（计划级
        // 去重）不受影响——两者是不同的东西。
        String table = "jdbc_it_distinct";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, name STRING)");
            s.executeUpdate("INSERT INTO " + table
                    + " VALUES (1, 'a'), (2, 'a'), (3, 'b')");

            // ① 聚合内 DISTINCT → 原生支持，值正确（曾回 3 的错值、中间代曾拒绝）
            try (ResultSet rs = s.executeQuery(
                    "SELECT COUNT(DISTINCT name) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(2L, rs.getLong("n"), "count(DISTINCT) 必须去重");
            }
            // ② 等价改写（旧内核的替代路径）仍可用——双形态并行，值一致
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM"
                    + " (SELECT DISTINCT name FROM " + table + ") s")) {
                assertTrue(rs.next());
                assertEquals(2L, rs.getLong("n"), "DISTINCT 子查询形态应与原生一致");
            }
            // ③ 标准 SELECT DISTINCT（计划级去重）照常
            try (ResultSet rs = s.executeQuery(
                    "SELECT DISTINCT name FROM " + table + " ORDER BY name")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void aggregateOverEmptyMatchReturnsZeroRow() throws SQLException {
        // 内核修复：count(x) 在无匹配时此前回**空数组**（调用方拿不到 0，
        // 只能当作"无结果"，是个坑），现正常回一行 0。
        String table = "jdbc_it_empty_agg";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, name STRING)");
            s.executeUpdate("INSERT INTO " + table + " VALUES (1, 'a')");

            try (ResultSet rs = s.executeQuery(
                    "SELECT COUNT(name) AS n FROM " + table + " WHERE id < 0")) {
                assertTrue(rs.next(), "聚合在无匹配时应回一行（而非空结果）");
                assertEquals(0L, rs.getLong("n"));
                assertFalse(rs.next());
            }
            // SUM/其他聚合同源；同时确认「真·无行」的结果集仍是 0 行（两者不同）
            try (ResultSet rs = s.executeQuery(
                    "SELECT name FROM " + table + " WHERE id < 0")) {
                assertFalse(rs.next(), "非聚合查询无匹配 → 0 行");
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void partitionedTableRequiresDataColumn() throws SQLException {
        // DDL 收紧：仅分区列的表被拒（分区列的值住在目录名里、不进数据文件，
        // 全分区列的表数据文件会是零列）。驱动的元数据/DDL 用例都带数据列，
        // 本用例把这条约束锁住，避免将来误加"纯分区列"形状的测试。
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            SQLException e = assertThrows(SQLException.class,
                    () -> s.execute("CREATE TABLE jdbc_it_pure_part (p BIGINT) PARTITION BY (p)"));
            assertEquals("42000", e.getSQLState()); // INVALID_ARGUMENT
            assertTrue(e.getMessage().contains("non-partition column"), e.getMessage());
            // 带数据列的照常（驱动所有分区表用例的形状）
            s.execute("CREATE TABLE jdbc_it_part_ok (id BIGINT, dt DATE) PARTITION BY (dt)");
        } finally {
            s.execute("DROP TABLE IF EXISTS jdbc_it_pure_part");
            s.execute("DROP TABLE IF EXISTS jdbc_it_part_ok");
            s.close();
            c.close();
        }
    }

    @Test
    void zeroRowResultsAcrossReadFormats() throws SQLException {
        // 零行结果是高频正常形态（LIMIT 0 探测元数据、WHERE 无命中）。内核
        // 曾对 rows 通道报 501 "empty batches"（反馈 J15），修复后两条路径
        // 都必须正常返回 0 行且**列集完整**。
        String[][] cases = { // {SQL, 期望列数}——须为**非聚合**形状：
                // 聚合在无匹配时回一行 0（见 aggregateOverEmptyMatchReturnsZeroRow），
                // 与"零行结果"是两回事
                {"SELECT 1 AS a, 'x' AS b LIMIT 0", "2"},
                {"SELECT 1 AS a WHERE false", "1"},
                {"SELECT id FROM (SELECT 1 AS id) t WHERE false", "1"},
        };
        for (String fmt : new String[] {"json", "rows"}) {
            Connection c = DriverManager.getConnection(url
                    + (url.contains("?") ? "&" : "?") + "readFormat=" + fmt);
            try (Statement s = c.createStatement()) {
                for (String[] qc : cases) {
                    try (ResultSet rs = s.executeQuery(qc[0])) {
                        assertEquals(Integer.parseInt(qc[1]), rs.getMetaData().getColumnCount(),
                                fmt + " / " + qc[0] + "：列集必须完整");
                        assertFalse(rs.next(), fmt + " / " + qc[0] + "：应无行");
                    }
                }
            }
            c.close();
        }
    }

    @Test
    void binaryColumnWritableViaSql() throws SQLException {
        // 内核修复（J15 同批）：INSERT VALUES 现接受字面量 CAST，BINARY 列
        // 可经 SQL 通道写入（此前只能走批量通道）
        String table = "jdbc_it_bin_sql";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, raw BINARY)");
            assertEquals(2, s.executeUpdate("INSERT INTO " + table + " VALUES"
                    + " (1, CAST('ab' AS BINARY)), (2, CAST('deadbeef' AS BINARY))"));
            // 语义记录：CAST(字符串 AS BINARY) 取该串的 **UTF-8 字节**
            // （不是 hex 解析）——"deadbeef" 是 8 字节文本
            try (ResultSet rs = s.executeQuery(
                    "SELECT raw FROM " + table + " WHERE id = 2")) {
                assertTrue(rs.next());
                byte[] b = rs.getBytes(1);
                assertEquals(8, b.length);
                assertEquals((byte) 'd', b[0]);
                assertEquals((byte) 'f', b[7]);
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void insertWithPartialColumnList() throws SQLException {
        // 内核修复同批：INSERT 列清单不再要求枚举全列（此前 3 列 vs 4 列表被拒），
        // 未列出的列取默认（NULL）
        String table = "jdbc_it_prefix_cols";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, name STRING, score DOUBLE)");
            assertEquals(1, s.executeUpdate(
                    "INSERT INTO " + table + " (id, name) VALUES (1, 'x')"));
            try (ResultSet rs = s.executeQuery(
                    "SELECT id, name, score FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong("id"));
                assertEquals("x", rs.getString("name"));
                assertNull(rs.getObject("score"));
                assertTrue(rs.wasNull());
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void readFormatJsonIsStablePath() throws SQLException {
        // readFormat=json 是"保持原样"的稳定路径：行为与本驱动引入 rows 前一致
        Connection c = DriverManager.getConnection(url
                + (url.contains("?") ? "&" : "?") + "readFormat=json");
        Statement s = c.createStatement();
        try (ResultSet rs = s.executeQuery("SELECT 1 AS a, 'x' AS b, NULL AS c")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("a"));
            assertEquals("x", rs.getString("b"));
            assertNull(rs.getString("c"));
            assertTrue(rs.wasNull());
        }
        s.close();
        c.close();
    }

    @Test
    void readFormatRejectsBadValue() {
        SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(
                url + (url.contains("?") ? "&" : "?") + "readFormat=xml"));
        assertTrue(e.getMessage().contains("readFormat"), e.getMessage());
    }

    @Test
    void decimalFullPrecision() throws SQLException {
        // J1 修复后 DECIMAL 以精确字符串到达——19 位以上精度往返（原 f64 通道
        // 只能保 15~16 位，此用例在旧内核上必失败）
        Connection c = connect();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(
                "SELECT CAST('12345678901234567.8901' AS DECIMAL(30, 4)) AS d");
        assertTrue(rs.next());
        assertEquals(new BigDecimal("12345678901234567.8901"), rs.getBigDecimal("d"));
        assertEquals(4, rs.getMetaData().getScale(1));
        assertEquals(30, rs.getMetaData().getPrecision(1));
        rs.close();
        s.close();
        c.close();
    }

    @Test
    void stringColumnKeepsNumericContent() throws SQLException {
        // J1 修复后 Utf8 恒 string：'123' 不再被改写成 number
        Connection c = connect();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT '123' AS s, '00123' AS s2");
        assertTrue(rs.next());
        assertEquals("123", rs.getString("s"));
        assertEquals("00123", rs.getString("s2")); // 前导零原样保留
        rs.close();
        s.close();
        c.close();
    }

    @Test
    void nullSemantics() throws SQLException {
        Connection c = connect();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT NULL AS n, 1 AS v");
        assertTrue(rs.next());
        assertNull(rs.getString("n"));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt("n"));
        assertTrue(rs.wasNull());
        assertEquals(1, rs.getInt("v"));
        assertFalse(rs.wasNull());
        rs.close();
        s.close();
        c.close();
    }

    @Test
    void parameterizedQuery() throws SQLException {
        // PreparedStatement：客户端渲染 → 内核执行（§七 路径）
        Connection c = connect();
        PreparedStatement ps = c.prepareStatement(
                "SELECT ? AS i, ? AS s, ? AS d, ? AS b, ? AS dt, ? AS ts, ? AS n");
        ps.setLong(1, 99L);
        ps.setString(2, "it's o'clock");
        ps.setDouble(3, 2.75d);
        ps.setBoolean(4, true);
        ps.setDate(5, java.sql.Date.valueOf("2026-01-02"));
        ps.setTimestamp(6, java.sql.Timestamp.valueOf("2026-01-02 03:04:05.123456789"));
        ps.setNull(7, java.sql.Types.BIGINT);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(99L, rs.getLong("i"));
        assertEquals("it's o'clock", rs.getString("s"));
        assertEquals(2.75d, rs.getDouble("d"), 0d);
        assertTrue(rs.getBoolean("b"));
        assertEquals(java.sql.Date.valueOf("2026-01-02"), rs.getDate("dt"));
        assertEquals(java.sql.Timestamp.valueOf("2026-01-02 03:04:05.123456789"),
                rs.getTimestamp("ts"));
        assertNull(rs.getObject("n"));
        assertTrue(rs.wasNull());
        rs.close();
        ps.close();
        c.close();
    }

    @Test
    void insertDdlRoundtrip() throws SQLException {
        // 2026-10-13 能力面对齐引擎：CREATE → 参数化 INSERT → 回读 → 多行
        // → executeBatch → 分区 DELETE → DROP 全链路（对真内核写路径）
        String table = "jdbc_it_roundtrip";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            assertFalse(s.execute("CREATE TABLE " + table
                    + " (id BIGINT, name STRING, score DOUBLE, dt DATE)"));
            assertEquals(0, s.getUpdateCount());
            // 参数化 INSERT（客户端渲染路径，§七）
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?, ?, ?)")) {
                ps.setLong(1, 1L);
                ps.setString(2, "alice");
                ps.setDouble(3, 91.5d);
                ps.setDate(4, java.sql.Date.valueOf("2026-01-01"));
                assertEquals(1, ps.executeUpdate());
            }
            // 多行 VALUES + 回读比对
            assertEquals(2, s.executeUpdate("INSERT INTO " + table
                    + " VALUES (2, 'bob', 85.0, DATE '2026-01-01'),"
                    + " (3, 'carol', 77.5, DATE '2026-01-02')"));
            try (ResultSet rs = s.executeQuery("SELECT id, name, score, dt FROM "
                    + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong("id"));
                assertEquals("alice", rs.getString("name"));
                assertEquals(91.5d, rs.getDouble("score"), 0d);
                assertEquals(java.sql.Date.valueOf("2026-01-01"), rs.getDate("dt"));
                assertTrue(rs.next());
                assertEquals("bob", rs.getString("name"));
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
            // executeBatch（逐条执行的诚实实现）
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?, ?, ?)")) {
                for (int i = 0; i < 3; i++) {
                    ps.setLong(1, 10L + i);
                    ps.setString(2, "batch" + i);
                    ps.setDouble(3, 60.0d + i);
                    ps.setDate(4, java.sql.Date.valueOf("2026-01-03"));
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(3, counts.length);
                assertEquals(1, counts[0]);
                assertEquals(1, counts[2]);
            }
            // 聚合列别名（内核 J13 已修；此前为避免内核别名丢失而按索引取值，
            // 现恢复按别名——见 aggregateProjectionContracts 的回归锁定）
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(6L, rs.getLong("n"));
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table); // 清理（幂等）
            s.close();
            c.close();
        }
    }

    @Test
    void partitionDeleteAndDdlCacheInvalidation() throws SQLException {
        // 分区粒度 DELETE（引擎支持面）+ DDL 后元数据缓存失效（§十一）
        String table = "jdbc_it_partition";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, dt DATE) PARTITION BY (dt)");
            assertEquals(3, s.executeUpdate("INSERT INTO " + table
                    + " VALUES (1, DATE '2026-02-01'), (2, DATE '2026-02-01'),"
                    + " (3, DATE '2026-02-02')"));
            // DDL 后缓存失效：DDL 前翻一次元数据，DDL 后再翻应看到新列
            c.getMetaData().getColumns(null, null, table, "%").close();
            s.execute("ALTER TABLE " + table + " ADD COLUMN extra BIGINT");
            java.sql.ResultSet colsRs = c.getMetaData().getColumns(null, null, table, "%");
            int cols = 0;
            boolean sawExtra = false;
            while (colsRs.next()) {
                cols++;
                sawExtra |= "extra".equals(colsRs.getString("COLUMN_NAME"));
            }
            colsRs.close();
            assertTrue(sawExtra, "metadata cache must be invalidated after DDL (cols=" + cols + ")");
            // 分区粒度 DELETE 生效；非分区键谓词 → 引擎 501 → 0A000
            assertEquals(2, s.executeUpdate(
                    "DELETE FROM " + table + " WHERE dt = DATE '2026-02-01'"));
            SQLException rowLevel = assertThrows(SQLException.class,
                    () -> s.executeUpdate("DELETE FROM " + table + " WHERE id = 3"));
            assertEquals("0A000", rowLevel.getSQLState());
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void batchMergeInsertsAllRowsWithCorrectCounts() throws SQLException {
        // 阶段1：executeBatch 合并为多行 VALUES——数据必须逐字保真、
        // 返回数组必须与原批条目逐项对应（值里刻意放逗号/括号/引号/关键字）
        String table = "jdbc_it_batch";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, name STRING)");
            int n = 500;
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?)")) {
                for (int i = 0; i < n; i++) {
                    ps.setLong(1, i);
                    ps.setString(2, "v," + i + " 'q' (p)"); // 逗号/引号/括号
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(n, counts.length);
                for (int i = 0; i < n; i++) {
                    assertEquals(1, counts[i], "batch entry " + i);
                }
            }
            try (ResultSet rs = s.executeQuery(
                    "SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(500L, rs.getLong("n"));
            }
            // 抽查内容保真（合并是文本拼接，值错位会立刻暴露）
            try (ResultSet rs = s.executeQuery(
                    "SELECT name FROM " + table + " WHERE id = 123")) {
                assertTrue(rs.next());
                assertEquals("v,123 'q' (p)", rs.getString(1));
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void batchRewriteDisabledFallsBackToPerStatement() throws SQLException {
        // 逃生舱：batchRewrite=false → 逐条执行，数据与计数必须与合并路径一致
        String table = "jdbc_it_norewrite";
        Properties p = new Properties();
        if (token != null) {
            p.setProperty("token", token);
        }
        p.setProperty("batchRewrite", "false");
        Connection c = DriverManager.getConnection("jdbc:qsdb://" + addr, p);
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, name STRING)");
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?)")) {
                for (int i = 0; i < 50; i++) {
                    ps.setLong(1, i);
                    ps.setString(2, "n" + i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(50, counts.length);
                for (int i = 0; i < 50; i++) {
                    assertEquals(1, counts[i]);
                }
            }
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(50L, rs.getLong("n"));
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void statementBatchMergeMixedWithOpaque() throws SQLException {
        // Statement.addBatch 路径：INSERT 合并 + 混入非 INSERT 条目原样执行
        String table = "jdbc_it_stmtbatch";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.addBatch("CREATE TABLE " + table + " (id BIGINT)");
            for (int i = 0; i < 20; i++) {
                s.addBatch("INSERT INTO " + table + " VALUES (" + i + ")");
            }
            int[] counts = s.executeBatch();
            assertEquals(21, counts.length);
            assertEquals(0, counts[0]);   // DDL 无行数
            assertEquals(1, counts[1]);
            assertEquals(1, counts[20]);
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(20L, rs.getLong("n"));
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void executeUpdateSemantics() throws SQLException {
        Connection c = connect();
        Statement s = c.createStatement();
        // 行级更新族拒绝（存储模型级不支持，引擎亦 501）
        SQLFeatureNotSupportedException e =
                assertThrows(SQLFeatureNotSupportedException.class,
                        () -> s.executeUpdate("UPDATE t SET a = 1"));
        assertEquals("0A000", e.getSQLState());
        // SELECT 走 executeUpdate → 规范错误（产生结果集）
        assertThrows(SQLException.class, () -> s.executeUpdate("SELECT 1"));
        s.close();
        c.close();
    }

    @Test
    void errorMapping() throws SQLException {
        Connection c = connect();
        Statement s = c.createStatement();
        // 语法错误 → PARSE_ERROR → 42601（码表精确映射；曾是笼统的 42000）
        SQLException syntax = assertThrows(SQLException.class,
                () -> s.executeQuery("SELECTT 1"));
        assertEquals("42601", syntax.getSQLState());
        assertTrue(syntax.getMessage().contains("code=PARSE_ERROR"), syntax.getMessage());
        // 查询端点表不存在 → TABLE_NOT_FOUND → 42P01（HTTP 400，非 404）
        SQLException table = assertThrows(SQLException.class,
                () -> s.executeQuery("SELECT * FROM no_such_table_xyz"));
        assertEquals("42P01", table.getSQLState());
        assertTrue(table.getMessage().contains("code=TABLE_NOT_FOUND"), table.getMessage());
        // 非表/列的不存在对象（函数名写错）→ INVALID_ARGUMENT → 42000（码变更对账）
        SQLException fn = assertThrows(SQLException.class,
                () -> s.executeQuery("SELECT no_such_function_xyz(1)"));
        assertTrue(fn.getMessage().contains("code=INVALID_ARGUMENT"), fn.getMessage());
        // 连接未因错误失效（§十：无状态协议，08S01 之外不关连接）
        assertFalse(c.isClosed());
        try (ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
        s.close();
        c.close();
    }

    @Test
    void timeoutHeaderAccepted() throws SQLException {
        // setQueryTimeout → X-QSDB-Timeout-Ms 头被内核接受；码表映射
        // （QUERY_TIMEOUT → HYT00，驱动契约覆盖内核建议 57014）由
        // errorMapping 的 code 分支与本地兜底表单测共同覆盖
        Connection c = connect();
        Statement s = c.createStatement();
        s.setQueryTimeout(60);
        try (ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
        s.close();
        c.close();
    }

    @Test
    void clientQueryIdAdoptedByServer() throws SQLException {
        // J11：驱动生成的 query id（jdbc- 前缀）被 server 采纳并回显——
        // QsdbStatement#getLastQueryId() 即响应回显值
        Connection c = connect();
        QsdbStatement s = (QsdbStatement) c.createStatement();
        try (ResultSet rs = s.executeQuery("SELECT 1 AS one")) {
            assertTrue(rs.next());
        }
        String echoed = s.getLastQueryId();
        assertNotNull(echoed, "server should echo queryId");
        assertTrue(echoed.startsWith("jdbc-"),
                "server should adopt client-specified id, got: " + echoed);
        s.close();
        c.close();
    }

    @Test
    void queryIdExposedAndCancelIsSafe() throws SQLException {
        // J4：响应带 queryId（诊断面）；cancel 在无执行中语句时为安全 no-op
        Connection c = connect();
        Statement s = c.createStatement();
        try (ResultSet rs = s.executeQuery("SELECT 1 AS one")) {
            assertTrue(rs.next());
        }
        // lastQueryId 为包内可见——这里只能验证 cancel 不抛（no-op 路径）
        s.cancel();
        // 执行后 cancel 已结束的查询同样安全
        try (ResultSet rs = s.executeQuery("SELECT 1")) {
            s.cancel(); // 结果集已完整到达（JSON 一次性），取消是 no-op
            assertTrue(rs.next());
        }
        s.close();
        c.close();
    }

    @Test
    void authFailure() throws SQLException {
        // 仅当 server 以 --token 启动时才有 401 路径（开放模式忽略任意 token）
        assumeTrue(token != null && !token.isEmpty(),
                "QSDB_IT_TOKEN not set — server is in open mode, auth path not exercised");
        Connection bad = DriverManager.getConnection("jdbc:qsdb://" + addr + "?token=WRONG-TOKEN");
        SQLException e = assertThrows(SQLException.class,
                () -> bad.createStatement().executeQuery("SELECT 1"));
        assertEquals("28000", e.getSQLState()); // 认证失败（§十）
        // 二次使用：authFailed 标记生效
        SQLException again = assertThrows(SQLException.class,
                () -> bad.createStatement().executeQuery("SELECT 1"));
        assertEquals("28000", again.getSQLState());
        bad.close();
    }

    @Test
    void maxRowsAndTimeouts() throws SQLException {
        Connection c = connect();
        Statement s = c.createStatement();
        s.setMaxRows(3);
        ResultSet rs = s.executeQuery(
                "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3"
                        + " UNION ALL SELECT 4 UNION ALL SELECT 5) t");
        int n = 0;
        while (rs.next()) {
            n++;
        }
        assertEquals(3, n, "driver-side truncation at maxRows");
        rs.close();
        // 超时参数下发不报错（正/零边界）
        s.setQueryTimeout(60);
        try (ResultSet r2 = s.executeQuery("SELECT 1")) {
            assertTrue(r2.next());
        }
        s.close();
        c.close();
    }

    @Test
    void databaseMetaData() throws SQLException {
        Connection c = connect();
        java.sql.DatabaseMetaData md = c.getMetaData();
        assertEquals("QSDB", md.getDatabaseProductName());
        assertNotNull(md.getDatabaseProductVersion());
        assertEquals("QSDB JDBC Driver", md.getDriverName());
        assertFalse(md.supportsTransactions());
        assertFalse(md.supportsSelectForUpdate());
        assertTrue(md.supportsBatchUpdates()); // executeBatch 逐条执行实现
        assertTrue(md.supportsUnion());
        assertTrue(md.supportsSubqueriesInExists());
        // getSchemas：单行 default
        try (ResultSet rs = md.getSchemas()) {
            assertTrue(rs.next());
            assertEquals("default", rs.getString("TABLE_SCHEM"));
            assertFalse(rs.next());
        }
        // getCatalogs：挂载库清单（P2 多库；测试 server 恒有 default + app，
        // 服务端按名序返回 app 在前）
        java.util.Set<String> cats = new java.util.HashSet<String>();
        try (ResultSet rs = md.getCatalogs()) {
            while (rs.next()) {
                cats.add(rs.getString("TABLE_CAT"));
            }
        }
        assertTrue(cats.contains("default") && cats.contains("app"), "挂载库全列: " + cats);
        // getTableTypes：["TABLE"]
        try (ResultSet rs = md.getTableTypes()) {
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        }
        // getTypeInfo：覆盖全类型行（§八）
        try (ResultSet rs = md.getTypeInfo()) {
            int rows = 0;
            boolean sawBigint = false;
            while (rs.next()) {
                rows++;
                if ("BIGINT".equals(rs.getString("TYPE_NAME"))) {
                    sawBigint = true;
                    assertEquals(java.sql.Types.BIGINT, rs.getInt("DATA_TYPE"));
                }
            }
            assertTrue(rows >= 13, "getTypeInfo rows: " + rows);
            assertTrue(sawBigint);
        }
        // 空集方法
        try (ResultSet rs = md.getPrimaryKeys(null, null, "any")) {
            assertFalse(rs.next());
        }
        try (ResultSet rs = md.getProcedures(null, null, "%")) {
            assertFalse(rs.next());
        }
        c.close();
    }

    @Test
    void tableMetadataIfAnyTableExists() throws SQLException {
        Connection c = connect();
        java.sql.DatabaseMetaData md = c.getMetaData();
        java.util.List<String> tables = new java.util.ArrayList<String>();
        try (ResultSet rs = md.getTables(null, null, "%", null)) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            }
        }
        if (tables.isEmpty()) {
            c.close();
            return; // 空库：表级元数据用例跳过
        }
        String first = tables.get(0);
        try (ResultSet rs = md.getColumns(null, null, first, "%")) {
            assertTrue(rs.next(), "columns of " + first);
            int lastOrdinal = 0;
            do {
                assertTrue(rs.getInt("ORDINAL_POSITION") > lastOrdinal);
                lastOrdinal = rs.getInt("ORDINAL_POSITION");
                assertNotNull(rs.getString("TYPE_NAME"));
                // java.sql.Types 的多数值本身是负数（BIGINT=-5、TINYINT=-6、
                // BINARY=-2），故只能断言"非零"（0 = Types.NULL，本驱动不产出）
                assertTrue(rs.getInt("DATA_TYPE") != 0,
                        "DATA_TYPE for " + rs.getString("COLUMN_NAME"));
            } while (rs.next());
        }
        c.close();
    }

    @Test
    void statementLevelMaxRowsOverridesConnection() throws SQLException {
        Properties p = new Properties();
        if (token != null) {
            p.setProperty("token", token);
        }
        p.setProperty("maxRows", "2");
        Connection c = DriverManager.getConnection("jdbc:qsdb://" + addr, p);
        Statement s = c.createStatement();
        try (ResultSet rs = s.executeQuery(
                "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t")) {
            int n = 0;
            while (rs.next()) {
                n++;
            }
            assertEquals(2, n, "connection-level maxRows");
        }
        s.setMaxRows(1); // 语句级覆盖
        try (ResultSet rs = s.executeQuery(
                "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2) t")) {
            int n = 0;
            while (rs.next()) {
                n++;
            }
            assertEquals(1, n);
        }
        s.close();
        c.close();
    }

    @Test
    void resultSetMetadataOfLiteralQuery() throws SQLException {
        // LIMIT 0 元数据（§十三.3）：0 行但 schema 完整。
        // 观察记录：内核把小数字面量 1.5 物化为 Decimal128（精确语义）而非
        // Float64——浮点需显式 CAST AS FLOAT/DOUBLE（驱动反馈 J10 附注）
        Connection c = connect();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(
                "SELECT 1 AS id, 'x' AS name, CAST(1.5 AS DOUBLE) AS v LIMIT 0");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount());
        assertEquals("id", md.getColumnLabel(1));
        assertEquals(java.sql.Types.BIGINT, md.getColumnType(1));
        assertEquals(java.sql.Types.VARCHAR, md.getColumnType(2));
        assertEquals(java.sql.Types.DOUBLE, md.getColumnType(3));
        assertFalse(rs.next());
        rs.close();
        s.close();
        c.close();
    }

    // ------------------------------------------------------------------
    // P0 协议对账（2026-10-19，内核十六批～二十批）
    // ------------------------------------------------------------------

    @Test
    void serverElapsedMsSurfacesFromResponseHeader() throws SQLException {
        // 十九批Ⅷ：统一响应头 X-QSDB-Elapsed-Ms（仅内核执行段耗时）。
        // 顺带覆盖 X-QSDB-Timeout-Ms 头路径（setQueryTimeout 现真下发——
        // 此前三处 javadoc 宣称但从未发送，2026-10-19 修复）
        Connection c = connect();
        Statement s = c.createStatement();
        s.setQueryTimeout(30);
        try (ResultSet rs = s.executeQuery("SELECT 1 AS a")) {
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong("a"));
        }
        Double elapsed = ((QsdbStatement) s).getServerElapsedMs();
        assertNotNull(elapsed, "server 应回 X-QSDB-Elapsed-Ms 头；null = 头未被消费");
        assertTrue(elapsed >= 0d, "elapsed must be non-negative, got " + elapsed);
        s.close();
        c.close();
    }

    @Test
    void maxRowsDriverTruncationAndExplicitUnlimitedLiftsGuard() throws SQLException {
        // 语义对账（2026-10-19 集成实测纠正）：内核 body maxRows 是结果护栏的
        // **超限拒绝**阈值（R1 绝不部分返回），与 JDBC setMaxRows 的**截断**
        // 语义相反——故 n>0 不上线（驱动侧截断）；仅显式 setMaxRows(0)
        // （JDBC"不限"）发 maxRows:0 解除本请求护栏
        String table = "jdbc_it_maxrows";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT)");
            assertEquals(10, s.executeUpdate("INSERT INTO " + table
                    + " VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)"));
            s.setMaxRows(3);
            try (ResultSet rs = s.executeQuery("SELECT id FROM " + table)) {
                int n = 0;
                while (rs.next()) {
                    n++;
                }
                assertEquals(3, n, "驱动侧截断（JDBC 语义）；若内核把 3 当拒绝阈值，"
                        + "10 行结果会在这里报 RESULT_TOO_LARGE");
            }
            s.setMaxRows(0); // 显式"不限" → body maxRows:0 解除护栏
            try (ResultSet rs = s.executeQuery("SELECT id FROM " + table)) {
                int n = 0;
                while (rs.next()) {
                    n++;
                }
                assertEquals(10, n, "显式 0 = 本请求不限（护栏解除）");
            }
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void updateCountAfterExecuteReflectsDmlRows() throws SQLException {
        // 2026-10-19 规范修复：execute() 后 getUpdateCount() 对 DML 返回真实
        // 受影响行数（此前恒 0，与 JDBC 规范不符）；DDL 为 0；查询为 -1
        String table = "jdbc_it_updcount";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            assertFalse(s.execute("CREATE TABLE " + table + " (id BIGINT)"));
            assertEquals(0, s.getUpdateCount(), "DDL：无行数概念 → 0");
            assertFalse(s.execute("INSERT INTO " + table + " VALUES (1), (2), (3)"));
            assertEquals(3, s.getUpdateCount(), "execute() 后 DML 行数必须可见");
            assertEquals(3L, s.getLargeUpdateCount());
            assertTrue(s.execute("SELECT COUNT(*) AS n FROM " + table));
            assertEquals(-1, s.getUpdateCount(), "当前结果是结果集 → -1");
            assertNotNull(s.getResultSet());
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    // ------------------------------------------------------------------
    // P1 服务端参数化通道（2026-10-19，paramStyle=server 默认）
    // ------------------------------------------------------------------

    /** 两通道写同一批值，逐列逐值差分比对（server vs text）。 */
    private static void writeViaChannelsAndCompare(String table, String textTable)
            throws SQLException {
        try (Connection server = connect();
             Connection text = DriverManager.getConnection(
                     url + (url.contains("?") ? "&" : "?") + "paramStyle=text")) {
            String cols = "(id BIGINT, name STRING, score DOUBLE, dt DATE)";
            try (Statement ss = server.createStatement();
                 Statement ts = text.createStatement()) {
                ss.execute("DROP TABLE IF EXISTS " + table);
                ts.execute("DROP TABLE IF EXISTS " + textTable);
                ss.execute("CREATE TABLE " + table + cols);
                ts.execute("CREATE TABLE " + textTable + cols);
            }
            String ins = "INSERT INTO %s VALUES (?, ?, ?, ?)";
            try (PreparedStatement ps = server.prepareStatement(String.format(ins, table))) {
                ps.setLong(1, 7L);
                ps.setString(2, "它's \"quoted\"");
                ps.setDouble(3, 2.75d);
                ps.setDate(4, java.sql.Date.valueOf("2026-02-03"));
                assertEquals(1, ps.executeUpdate());
                ps.setLong(1, 8L);
                ps.setString(2, null);
                ps.setDouble(3, -0.5d);
                ps.setDate(4, null);
                ps.setNull(4, java.sql.Types.DATE);
                assertEquals(1, ps.executeUpdate());
            }
            try (PreparedStatement ps = text.prepareStatement(String.format(ins, textTable))) {
                ps.setLong(1, 7L);
                ps.setString(2, "它's \"quoted\"");
                ps.setDouble(3, 2.75d);
                ps.setDate(4, java.sql.Date.valueOf("2026-02-03"));
                assertEquals(1, ps.executeUpdate());
                ps.setLong(1, 8L);
                ps.setString(2, null);
                ps.setDouble(3, -0.5d);
                ps.setNull(4, java.sql.Types.DATE);
                assertEquals(1, ps.executeUpdate());
            }
            // 差分：两表逐行逐列一致（含中文/引号/NULL/负小数）
            String q = "SELECT id, name, score, dt FROM %s ORDER BY id";
            try (ResultSet a = server.createStatement().executeQuery(String.format(q, table));
                 ResultSet b = server.createStatement().executeQuery(String.format(q, textTable))) {
                while (a.next() && b.next()) {
                    assertEquals(a.getLong(1), b.getLong(1));
                    assertEquals(a.getString(2), b.getString(2));
                    assertEquals(a.getDouble(3), b.getDouble(3), 0d);
                    assertEquals(a.getDate(4), b.getDate(4));
                }
                assertFalse(a.next());
                assertFalse(b.next());
            }
            // 差分：谓词参数化——server 通道 WHERE 过滤与 text 通道一致。
            // name 谓词命中 id=7、score 谓词命中 id=8（NULL name）——两通道
            // 返回的 id 集合必须一致
            String where = "SELECT id FROM %s WHERE name = ? OR score = ?";
            try (PreparedStatement pa = server.prepareStatement(String.format(where, table));
                 PreparedStatement pb = text.prepareStatement(String.format(where, textTable))) {
                pa.setString(1, "它's \"quoted\"");
                pa.setDouble(2, -0.5d);
                pb.setString(1, "它's \"quoted\"");
                pb.setDouble(2, -0.5d);
                java.util.Set<Long> idsA = new java.util.HashSet<Long>();
                java.util.Set<Long> idsB = new java.util.HashSet<Long>();
                try (ResultSet ra = pa.executeQuery()) {
                    while (ra.next()) {
                        idsA.add(ra.getLong(1));
                    }
                }
                try (ResultSet rb = pb.executeQuery()) {
                    while (rb.next()) {
                        idsB.add(rb.getLong(1));
                    }
                }
                assertEquals(new java.util.HashSet<Long>(java.util.Arrays.asList(7L, 8L)),
                        idsA, "name 与 score 两个谓词各命中一行");
                assertEquals(idsA, idsB, "两通道过滤结果一致");
            }
            try (Statement ss = server.createStatement();
                 Statement ts = text.createStatement()) {
                ss.execute("DROP TABLE IF EXISTS " + table);
                ts.execute("DROP TABLE IF EXISTS " + textTable);
            }
        }
    }

    @Test
    void paramsChannelMatchesTextChannel() throws SQLException {
        // P1 差分测试：同一批值经 server 通道（值不进 SQL 文本）与 text 通道
        // （客户端字面量渲染）写入/查询，结果必须逐值一致
        writeViaChannelsAndCompare("jdbc_it_params_a", "jdbc_it_params_b");
    }

    @Test
    void serverChannelUnlocksBinaryNonFiniteAndControlChars() throws SQLException {
        // P1 能力转正（文本通道此前拒绝的三类参数）：
        // ① setBytes（BINARY 列回读逐字节）；② NaN/±Infinity 浮点参数；
        // ③ 控制字符字符串（JSON 转义合法）。text 通道行为不变（NaN 仍拒）。
        String table = "jdbc_it_paramx";
        Connection c = connect();
        try {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
                s.execute("CREATE TABLE " + table
                        + " (id BIGINT, bin BINARY, d DOUBLE, s STRING)");
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?, ?, ?)")) {
                ps.setLong(1, 1L);
                ps.setBytes(2, new byte[] {0x00, 0x61, (byte) 0xff});
                ps.setDouble(3, Double.NaN);
                ps.setString(4, "a\nb\tc");
                assertEquals(1, ps.executeUpdate());
                ps.setLong(1, 2L);
                ps.setBytes(2, null);
                ps.setDouble(3, Double.POSITIVE_INFINITY);
                ps.setString(4, null);
                assertEquals(1, ps.executeUpdate());
            }
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT id, bin, d, s FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong("id"));
                byte[] bin = rs.getBytes("bin");
                assertEquals(3, bin.length);
                assertEquals(0x00, bin[0]);
                assertEquals(0x61, bin[1]);
                assertEquals((byte) 0xff, bin[2]);
                assertTrue(Double.isNaN(rs.getDouble("d")), "NaN 往返（JSON 通道拼写 NaN）");
                assertEquals("a\nb\tc", rs.getString("s"), "控制字符往返");
                assertTrue(rs.next());
                assertNull(rs.getBytes("bin"));
                assertTrue(rs.getDouble("d") == Double.POSITIVE_INFINITY, "+Inf 往返");
                assertFalse(rs.next());
            }
        } finally {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
            }
            c.close();
        }
        // text 通道行为保持：NaN 仍拒（SQL 无字面量形态）——不是回归
        try (Connection text = DriverManager.getConnection(
                url + (url.contains("?") ? "&" : "?") + "paramStyle=text");
             PreparedStatement ps = text.prepareStatement("SELECT ?")) {
            ps.setDouble(1, Double.NaN);
            assertThrows(SQLException.class, ps::executeQuery,
                    "text 通道 NaN 仍拒绝（SQL 无字面量形态）");
        }
    }

    @Test
    void textChannelBinaryLiteralRoundtrip() throws SQLException {
        // text 通道 setBytes → X'…' 字面量（内核第九轮起支持）——旧行为
        // "v1 unsupported" 已随内核能力面解除
        String table = "jdbc_it_textbin";
        try (Connection text = DriverManager.getConnection(
                url + (url.contains("?") ? "&" : "?") + "paramStyle=text")) {
            try (Statement s = text.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
                s.execute("CREATE TABLE " + table + " (bin BINARY)");
            }
            try (PreparedStatement ps = text.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?)")) {
                ps.setBytes(1, new byte[] {(byte) 0xde, (byte) 0xad});
                assertEquals(1, ps.executeUpdate());
            }
            try (Statement s = text.createStatement();
                 ResultSet rs = s.executeQuery("SELECT bin FROM " + table)) {
                assertTrue(rs.next());
                byte[] bin = rs.getBytes(1);
                assertEquals(2, bin.length);
                assertEquals((byte) 0xde, bin[0]);
                assertEquals((byte) 0xad, bin[1]);
            }
            try (Statement s = text.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    // ------------------------------------------------------------------
    // P2 login 会话 + 多库 catalog（2026-10-19，内核十八批 C1 / 十六批 M-B）
    // 需要 server 带 --mount app=<dir> 与预建用户 qsadmin/itpass（admin 角色）
    // ------------------------------------------------------------------

    private static final String IT_USER = "qsadmin";
    private static final String IT_PASSWORD = "itpass";
    private static final String IT_MOUNT = "app";

    private static String loginUrl(String extra) {
        String base = "jdbc:qsdb://" + addr + "?user=" + IT_USER + "&password=" + IT_PASSWORD;
        return extra == null ? base : base + "&" + extra;
    }

    @Test
    void loginSessionFlow() throws SQLException {
        // user+password → Basic → /auth/login → 进程态会话 token（十八批 C1）
        try (Connection c = DriverManager.getConnection(loginUrl(null))) {
            assertTrue(c.isValid(5), "login 连接可用");
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT 1 AS a")) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong("a"));
            }
            // 写语句走会话 token（回执结构化字段照常）
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + IT_MOUNT + ".jdbc_it_sess");
                assertFalse(s.execute("CREATE TABLE " + IT_MOUNT + ".jdbc_it_sess (id BIGINT)"));
                assertEquals(1, s.executeUpdate(
                        "INSERT INTO " + IT_MOUNT + ".jdbc_it_sess VALUES (1)"));
                s.execute("DROP TABLE IF EXISTS " + IT_MOUNT + ".jdbc_it_sess");
            }
        }
    }

    @Test
    void loginBadPasswordIsPermanentAuthFailure() throws SQLException {
        // 凭证错（UNAUTHORIZED）→ 28000 且连接不可自愈（不与 SESSION_REVOKED
        // 混淆——后者由驱动自动重登恢复，凭证错才是永久死）
        try (Connection bad = DriverManager.getConnection(
                "jdbc:qsdb://" + addr + "?user=" + IT_USER + "&password=WRONG")) {
            SQLException e = assertThrows(SQLException.class,
                    () -> bad.createStatement().executeQuery("SELECT 1"));
            assertEquals("28000", e.getSQLState());
            SQLException again = assertThrows(SQLException.class,
                    () -> bad.createStatement().executeQuery("SELECT 1"));
            assertEquals("28000", again.getSQLState(), "凭证错不会自愈");
        }
    }

    @Test
    void passwordWithoutUserRejectedAtConnect() {
        // password 必须与 user 成对（P2 规范化：密码≠token 别名）
        SQLException e = assertThrows(SQLException.class,
                () -> DriverManager.getConnection("jdbc:qsdb://" + addr + "?password=xxx"));
        assertTrue(e.getMessage().contains("'user'"), e.getMessage());
    }

    @Test
    void catalogsListMountedDatabases() throws SQLException {
        try (Connection c = connect()) {
            java.util.Set<String> cats = new java.util.HashSet<String>();
            try (ResultSet rs = c.getMetaData().getCatalogs()) {
                while (rs.next()) {
                    cats.add(rs.getString(1));
                }
            }
            assertTrue(cats.contains("default"), "default 库恒在: " + cats);
            assertTrue(cats.contains(IT_MOUNT), "测试挂载库应在列: " + cats);
        }
    }

    @Test
    void setCatalogRoutesQueriesToDatabase() throws SQLException {
        // setCatalog("app") → 每查询 body database=app（优先级最高）——
        // 建表落在 app 库，default 库看不到
        String table = "jdbc_it_catalog";
        try (Connection c = connect()) {
            c.setCatalog(IT_MOUNT);
            assertEquals(IT_MOUNT, c.getCatalog());
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
                assertFalse(s.execute("CREATE TABLE " + table + " (id BIGINT)"));
                assertEquals(2, s.executeUpdate(
                        "INSERT INTO " + table + " VALUES (1), (2)"));
            }
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(2L, rs.getLong("n"));
            }
            // default 库的连接看不到 app 库的表（隔离成立）
            try (Connection d = connect();
                 ResultSet rs = d.getMetaData().getTables("default", null, table, null)) {
                assertFalse(rs.next(), "app 库的表不得出现在 default catalog");
            }
            // 显式限定名（app.table）照常可用——多库 SQL 限定符
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT COUNT(*) AS n FROM " + IT_MOUNT + "." + table)) {
                assertTrue(rs.next());
                assertEquals(2L, rs.getLong("n"));
            }
        } finally {
            try (Statement s = connect().createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + IT_MOUNT + "." + table);
            }
        }
    }

    @Test
    void databasePropertyIsDefaultCatalog() throws SQLException {
        // 连接属性 database=app → setCatalog 未调用时查询也路由到 app
        String table = "jdbc_it_dbprop";
        try (Connection c = DriverManager.getConnection(
                loginUrl("database=" + IT_MOUNT))) {
            assertNull(c.getCatalog(), "未显式 setCatalog → getCatalog 为 null（规范）");
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
                assertFalse(s.execute("CREATE TABLE " + table + " (id BIGINT)"));
            }
            try (Connection d = connect();
                 ResultSet rs = d.getMetaData().getTables(IT_MOUNT, null, table, null)) {
                assertTrue(rs.next(), "database 属性生效：表落在 app 库");
                assertEquals(IT_MOUNT, rs.getString("TABLE_CAT"));
                assertFalse(rs.next());
            }
        } finally {
            try (Statement s = connect().createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + IT_MOUNT + "." + table);
            }
        }
    }

    @Test
    void getTablesAndColumnsFilterByCatalog() throws SQLException {
        // TABLE_CAT 填库名；catalog 参数过滤（null = 全部）
        String table = "jdbc_it_catfilter";
        try (Connection c = connect()) {
            c.setCatalog(IT_MOUNT);
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
                s.execute("CREATE TABLE " + table + " (id BIGINT, name STRING)");
            }
            try (ResultSet rs = c.getMetaData().getTables(null, null, table, null)) {
                assertTrue(rs.next());
                assertEquals(IT_MOUNT, rs.getString("TABLE_CAT"), "TABLE_CAT = 库名");
                assertEquals(table, rs.getString("TABLE_NAME"), "TABLE_NAME 无限定前缀");
                assertFalse(rs.next());
            }
            try (ResultSet rs = c.getMetaData().getTables(IT_MOUNT, null, table, null)) {
                assertTrue(rs.next(), "按 app catalog 过滤命中");
            }
            try (ResultSet rs = c.getMetaData().getTables("no_such_db", null, table, null)) {
                assertFalse(rs.next(), "不存在的 catalog → 空集");
            }
            try (ResultSet rs = c.getMetaData().getColumns(null, null, table, "%")) {
                assertTrue(rs.next());
                assertEquals(IT_MOUNT, rs.getString("TABLE_CAT"));
                assertEquals("id", rs.getString("COLUMN_NAME"));
                assertTrue(rs.next());
                assertEquals("name", rs.getString("COLUMN_NAME"));
                assertFalse(rs.next());
            }
        } finally {
            try (Statement s = connect().createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + IT_MOUNT + "." + table);
            }
        }
    }

    // ------------------------------------------------------------------
    // P3 真流式（setFetchSize）+ overwrite/CAS 写入（2026-10-19）
    // ------------------------------------------------------------------

    @Test
    void streamingMatchesOneShotAndTruncationAborts() throws SQLException {
        // P3 差分：fetchSize>0（rows 流式通道）与一次性通道逐值一致；
        // maxRows 截断在流式下 = 读满即关流断链；DML 回执经流式连接一次性交付
        String table = "jdbc_it_stream";
        Connection c = connect();
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table + " (id BIGINT, v STRING)");
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?)")) {
                for (int i = 1; i <= 20000; i++) {
                    ps.setLong(1, i);
                    ps.setString(2, "v" + i);
                    ps.addBatch();
                }
                assertEquals(20000, ps.executeBatch().length);
            }
            // 一次性基线
            long total;
            try (ResultSet rs = s.executeQuery("SELECT id, v FROM " + table + " ORDER BY id")) {
                total = 0;
                long first = -1;
                long last = -1;
                while (rs.next()) {
                    if (first < 0) {
                        first = rs.getLong(1);
                    }
                    last = rs.getLong(1);
                    total++;
                }
                assertEquals(20000, total);
                assertEquals(1L, first);
                assertEquals(20000L, last);
            }
            // 流式：同查询逐块读回，抽样逐值一致、总行数一致
            s.setFetchSize(500);
            try (ResultSet rs = s.executeQuery("SELECT id, v FROM " + table + " ORDER BY id")) {
                long n = 0;
                while (rs.next()) {
                    n++;
                    if (n <= 3 || n == 20000) {
                        assertEquals(n, rs.getLong(1));
                        assertEquals("v" + n, rs.getString(2));
                    }
                }
                assertEquals(total, n, "流式与一次性行数一致");
            }
            // 流式 + maxRows：读满即关流断链（剩余传输作废，不报错）
            s.setMaxRows(100);
            try (ResultSet rs = s.executeQuery("SELECT id FROM " + table + " ORDER BY id")) {
                int n = 0;
                while (rs.next()) {
                    n++;
                }
                assertEquals(100, n);
            }
            s.setMaxRows(0);
            // DML 回执经流式连接：非 SELECT → JSON 回执一次性交付（magic 嗅探分流）
            assertFalse(s.execute("INSERT INTO " + table + " VALUES (999999, 'tail')"));
            assertEquals(1, s.getUpdateCount(), "流式连接上的 DML 回执行数可见");
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }

    @Test
    void bulkOverwriteCasRoundtrip() throws SQLException {
        // P3 CAS 写入（内核 F24）：overwriteCas(正确 generation) → 分区替换
        // 成功；过期 generation → WRITE_CONFLICT（40001，动作 = 重读再决定）
        String table = "jdbc_it_cas";
        QsdbConnection c = (QsdbConnection) connect(); // bulkWriter/getTablePartitions 是驱动扩展 API
        Statement s = c.createStatement();
        try {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table
                    + " (id BIGINT, dt DATE) PARTITION BY (dt)");
            java.sql.Date dt = java.sql.Date.valueOf("2026-05-01");
            try (QsdbBulkWriter w = c.bulkWriter(table)) {
                for (long i = 1; i <= 3; i++) {
                    w.setLong("id", i);
                    w.setDate("dt", dt);
                    w.addRow();
                }
            }
            long gen = Long.MIN_VALUE;
            for (cn.qinwh.qsdb.jdbc.transport.PartitionInfo p : c.getTablePartitions(table)) {
                if (p.name.contains("2026-05-01")) {
                    gen = p.generation;
                }
            }
            assertTrue(gen != Long.MIN_VALUE, "分区清单应含 2026-05-01");
            final long staleGen = gen; // lambda 捕获需有效 final（冲突用例故意用过期值）

            // CAS 正确 generation → 替换成功（3 行 → 5 行）
            try (QsdbBulkWriter w = c.bulkWriter(table).overwriteCas(staleGen)) {
                for (long i = 10; i <= 14; i++) {
                    w.setLong("id", i);
                    w.setDate("dt", dt);
                    w.addRow();
                }
            }
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) AS n FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(5L, rs.getLong("n"), "overwrite 整分区替换");
            }

            // 过期 generation → WRITE_CONFLICT 40001（同 SQLState 族与
            // STALE_WRITER 动作相反，靠 code 分流）
            SQLException conflict = assertThrows(SQLException.class, () -> {
                try (QsdbBulkWriter w = c.bulkWriter(table).overwriteCas(staleGen)) {
                    w.setLong("id", 99L);
                    w.setDate("dt", dt);
                    w.addRow();
                    w.flush();
                }
            });
            assertEquals("40001", conflict.getSQLState(),
                    "CAS 未命中 → 40001: " + conflict.getMessage());
            assertTrue(conflict.getMessage().contains("WRITE_CONFLICT"),
                    conflict.getMessage());
        } finally {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
            c.close();
        }
    }
}
