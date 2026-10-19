import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

/**
 * M1 验收 Demo（设计文档 §十四）：Java 8 单文件——DriverManager →
 * PreparedStatement → 全类型读取。
 *
 * <p>编译运行（无需任何第三方依赖，驱动 jar 即全部）：
 * <pre>
 *   javac -cp qsdb-jdbc-0.1.0.jar Demo.java
 *   java  -cp .:qsdb-jdbc-0.1.0.jar Demo 127.0.0.1:9430 $QSDB_TOKEN
 * </pre>
 */
public class Demo {

    public static void main(String[] args) throws Exception {
        String addr = args.length > 0 ? args[0] : "127.0.0.1:9430";
        String token = args.length > 1 ? args[1] : null;

        // 1) 连接：与连 MySQL 的业务写法零差异（JDBC 4 SPI 自动发现驱动）
        String url = "jdbc:qsdb://" + addr + (token != null && !token.isEmpty()
                ? "?token=" + token : "");
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("connected: " + conn.getMetaData().getDatabaseProductName()
                    + " " + conn.getMetaData().getDatabaseProductVersion()
                    + " (driver " + conn.getMetaData().getDriverVersion() + ")");

            // 2) 预处理语句：? 渲染为类型化字面量（类型安全 + 注入防护）
            String sql = "SELECT CAST(? AS TINYINT) AS a_tiny,"
                    + " CAST(? AS SMALLINT) AS a_small,"
                    + " CAST(? AS INT) AS a_int,"
                    + " ? AS a_big,"
                    + " CAST(? AS FLOAT) AS a_f32,"
                    + " ? AS a_f64,"
                    + " CAST(? AS DECIMAL(10,2)) AS a_dec,"
                    + " ? AS a_str,"
                    + " ? AS a_bool,"
                    + " ? AS a_date,"
                    + " ? AS a_ts,"
                    + " ? AS a_time,"
                    + " CAST('deadbeef' AS BINARY) AS a_bin,"
                    + " ? AS a_null";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setByte(1, (byte) 42);
                ps.setShort(2, (short) 300);
                ps.setInt(3, 70000);
                ps.setLong(4, 123456789012345L);
                ps.setFloat(5, 1.5f);
                ps.setDouble(6, 2.5d);
                ps.setBigDecimal(7, new BigDecimal("12.34"));
                ps.setString(8, "hello — it's quoted: '");
                ps.setBoolean(9, true);
                ps.setDate(10, Date.valueOf("2026-10-12"));
                ps.setTimestamp(11, Timestamp.valueOf("2026-10-12 08:30:00.123456789"));
                ps.setTime(12, java.sql.Time.valueOf("08:30:00"));
                ps.setNull(13, java.sql.Types.BIGINT);

                // 3) 全类型读取（wasNull 语义见 JDBC 规范）
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    System.out.println("TINYINT   = " + rs.getByte("a_tiny"));
                    System.out.println("SMALLINT  = " + rs.getShort("a_small"));
                    System.out.println("INT       = " + rs.getInt("a_int"));
                    System.out.println("BIGINT    = " + rs.getLong("a_big"));
                    System.out.println("FLOAT     = " + rs.getFloat("a_f32"));
                    System.out.println("DOUBLE    = " + rs.getDouble("a_f64"));
                    System.out.println("DECIMAL   = " + rs.getBigDecimal("a_dec")
                            + " (scale=" + rs.getMetaData().getScale(7) + ")");
                    System.out.println("VARCHAR   = " + rs.getString("a_str"));
                    System.out.println("BOOLEAN   = " + rs.getBoolean("a_bool"));
                    System.out.println("DATE      = " + rs.getDate("a_date"));
                    System.out.println("TIMESTAMP = " + rs.getTimestamp("a_ts"));
                    System.out.println("TIME      = " + rs.getTime("a_time"));
                    System.out.println("BINARY    = "
                            + javax.xml.bind.DatatypeConverter.printHexBinary(rs.getBytes("a_bin")));
                    rs.getInt("a_null");
                    System.out.println("NULL      = wasNull?" + rs.wasNull());
                }
            }

            // 4) 元数据：表/列（DatabaseMetaData，工具兼容面）
            java.sql.DatabaseMetaData md = conn.getMetaData();
            System.out.println("supportsTransactions=" + md.supportsTransactions()
                    + " supportsUnion=" + md.supportsUnion());
            int tables = 0;
            try (ResultSet rs = md.getTables(null, null, "%", null)) {
                while (rs.next() && tables < 10) {
                    System.out.println("table: " + rs.getString("TABLE_NAME"));
                    tables++;
                }
            }
            System.out.println("demo done");
        }
    }

    private static void assertTrue(boolean b) {
        if (!b) {
            throw new AssertionError();
        }
    }
}
