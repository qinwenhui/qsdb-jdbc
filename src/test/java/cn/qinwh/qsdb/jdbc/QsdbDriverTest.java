package cn.qinwh.qsdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

/**
 * Driver SPI：URL 解析 / 属性 / 注册。
 */
class QsdbDriverTest {

    @Test
    void spiRegistration() throws SQLException {
        // JDBC 4 SPI：DriverManager 自动发现（META-INF/services）
        QsdbDriver d = new QsdbDriver();
        assertTrue(d.acceptsURL("jdbc:qsdb://localhost:9430"));
        assertTrue(d.acceptsURL("jdbc:qsdb://localhost"));
        assertFalse(d.acceptsURL("jdbc:mysql://localhost"));
        assertFalse(d.acceptsURL(null));
        // DriverManager 能枚举到本驱动
        boolean found = false;
        for (java.sql.Driver dr : java.util.Collections.list(DriverManager.getDrivers())) {
            if (dr instanceof QsdbDriver) {
                found = true;
            }
        }
        assertTrue(found, "driver should be registered via SPI");
    }

    @Test
    void urlParsing() throws SQLException {
        assertEquals("myhost", QsdbDriver.ParsedUrl.parse("jdbc:qsdb://myhost").host);
        assertEquals(9430, QsdbDriver.ParsedUrl.parse("jdbc:qsdb://myhost").port); // 默认端口
        assertEquals(8123, QsdbDriver.ParsedUrl.parse("jdbc:qsdb://myhost:8123").port);
        assertEquals("::1", QsdbDriver.ParsedUrl.parse("jdbc:qsdb://[::1]:9430").host);
        QsdbDriver.ParsedUrl withProps =
                QsdbDriver.ParsedUrl.parse("jdbc:qsdb://h:1?token=abc&maxRows=100");
        assertEquals("abc", withProps.urlProps.getProperty("token"));
        assertEquals("100", withProps.urlProps.getProperty("maxRows"));
    }

    @Test
    void urlValidation() {
        assertThrows(SQLException.class, () -> QsdbDriver.ParsedUrl.parse("jdbc:qsdb://"));
        assertThrows(SQLException.class, () -> QsdbDriver.ParsedUrl.parse("jdbc:qsdb://h/db")); // 路径拒绝
        assertThrows(SQLException.class, () -> QsdbDriver.ParsedUrl.parse("jdbc:qsdb://h:port"));
        assertThrows(SQLException.class, () -> QsdbDriver.ParsedUrl.parse("jdbc:qsdb://h:99999"));
    }

    @Test
    void versioning() {
        QsdbDriver d = new QsdbDriver();
        assertEquals(0, d.getMajorVersion());
        assertEquals(1, d.getMinorVersion()); // 0.1.0
        assertFalse(d.jdbcCompliant()); // 诚实：只读、无事务（§四.2）
    }

    @Test
    void connectValidatesProperties() throws SQLException {
        QsdbDriver d = new QsdbDriver();
        Properties bad = new Properties();
        bad.setProperty("ssl", "true");
        SQLException e = assertThrows(SQLException.class,
                () -> d.connect("jdbc:qsdb://localhost:9430", bad));
        assertEquals("0A000", e.getSQLState());

        Properties badTransport = new Properties();
        badTransport.setProperty("transport", "flight");
        assertThrows(SQLException.class,
                () -> d.connect("jdbc:qsdb://localhost:9430", badTransport));

        Properties badInt = new Properties();
        badInt.setProperty("queryTimeoutMs", "abc");
        assertThrows(SQLException.class,
                () -> d.connect("jdbc:qsdb://localhost:9430", badInt));

        // 布尔属性严格解析：拼错即报错（对齐内核「不静默兜底」原则）
        Properties badBool = new Properties();
        badBool.setProperty("batchRewrite", "maybe");
        SQLException e2 = assertThrows(SQLException.class,
                () -> d.connect("jdbc:qsdb://localhost:9430", badBool));
        assertTrue(e2.getMessage().contains("batchRewrite"), e2.getMessage());
        // URL 参数同样严格（URL 优先于 Properties，§四.1）
        assertThrows(SQLException.class,
                () -> d.connect("jdbc:qsdb://localhost:9430?batchRewrite=nope", new Properties()));
        // 合法值双形态均接受
        d.connect("jdbc:qsdb://192.0.2.1:1?batchRewrite=false", new Properties()).close();
        d.connect("jdbc:qsdb://192.0.2.1:1?batchRewrite=TRUE", new Properties()).close();
    }

    @Test
    void connectDoesNotTouchNetwork() throws SQLException {
        // §四.2：connect 不预建网络连接——不可达地址也应成功返回逻辑连接
        QsdbDriver d = new QsdbDriver();
        java.sql.Connection c = d.connect("jdbc:qsdb://192.0.2.1:1", new Properties()); // TEST-NET-1
        assertNull(c.getCatalog());
        assertFalse(c.isClosed());
        assertEquals(true, c.getAutoCommit());
        assertEquals("QSDB", c.getMetaData().getDatabaseProductName());
        c.close();
        assertTrue(c.isClosed());
    }

    @Test
    void tokenNotInToString() throws SQLException {
        // §十二 评审红线：toString 不含 token
        QsdbDriver d = new QsdbDriver();
        Properties p = new Properties();
        p.setProperty("token", "SECRET-TOKEN-123");
        java.sql.Connection c = d.connect("jdbc:qsdb://localhost:9430", p);
        assertFalse(c.toString().contains("SECRET-TOKEN-123"), c.toString());
        assertFalse(c.getMetaData().getURL().contains("SECRET-TOKEN-123"));
        c.close();
    }

    @Test
    void dataSource() throws SQLException {
        QsdbDataSource ds = new QsdbDataSource("jdbc:qsdb://192.0.2.1:1");
        ds.setToken("t");
        java.sql.Connection c = ds.getConnection();
        assertFalse(c.isClosed());
        // user/password 变体：password 仅在未设 token 时生效
        java.sql.Connection c2 = ds.getConnection("report", "pw");
        assertFalse(c2.isClosed());
        c.close();
        c.close(); // close 幂等（§十一：连接池借还兼容性）
        c2.close();
        assertTrue(c.isClosed());
    }
}
