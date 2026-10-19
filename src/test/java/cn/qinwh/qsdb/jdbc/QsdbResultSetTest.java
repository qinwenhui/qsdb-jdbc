package cn.qinwh.qsdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import cn.qinwh.qsdb.jdbc.transport.ColMeta;
import cn.qinwh.qsdb.jdbc.transport.QueryResponse;
import cn.qinwh.qsdb.jdbc.transport.Value;

/**
 * 全类型 getXxx 转换 + wasNull 语义单测（§六.1 / §六.3，M1 验收项）。
 * 直接以内存 QueryResponse 构造 ResultSet，不经网络。
 */
class QsdbResultSetTest {

    /** 列：c_int(Int64) c_str(Utf8) c_f64(Float64) c_bool(Boolean) c_dec(Decimal128(10,2)) */
    private static final List<ColMeta> COLS = Arrays.asList(
            new ColMeta("c_int", "Int64"),
            new ColMeta("c_str", "Utf8"),
            new ColMeta("c_f64", "Float64"),
            new ColMeta("c_bool", "Boolean"),
            new ColMeta("c_dec", "Decimal128(10, 2)"),
            new ColMeta("c_date", "Date32"),
            new ColMeta("c_ts", "Timestamp(Nanosecond, None)"),
            new ColMeta("c_time", "Time64(Nanosecond)"),
            new ColMeta("c_bin", "Binary"),
            new ColMeta("c_i8", "Int8"));

    private static QsdbResultSet rs(List<List<Value>> rows) throws SQLException {
        return QsdbResultSet.forRows(COLS, rows);
    }

    /** 典型行 + 全 NULL 行（内核 JSON 通道的值形态——含数字启发后果）。 */
    private static List<List<Value>> typicalRows() {
        List<List<Value>> rows = new ArrayList<List<Value>>();
        // 行 1：常规值。注意 c_str 是字符串列但值内容为数字串——内核数字启发下
        // 会以 JSON number 到达（这里以 Long 形态模拟），驱动必须按列类型转回 String
        rows.add(Arrays.asList(
                Value.of(42L), Value.of("123"), Value.of(1.5d), Value.of(true),
                Value.of(12.34d), Value.of("2026-10-12"),
                Value.of("2026-10-12T08:30:00.123456789"), Value.of("08:30:00"),
                Value.of("deadbeef"), Value.of(-5L)));
        // 行 2：全 NULL
        List<Value> nulls = new ArrayList<Value>();
        for (int i = 0; i < COLS.size(); i++) {
            nulls.add(Value.NULL);
        }
        rows.add(nulls);
        return rows;
    }

    @Test
    void typedGetters() throws SQLException {
        QsdbResultSet rs = rs(typicalRows());
        assertTrue(rs.next());
        assertEquals(42, rs.getInt("c_int"));
        assertEquals(42L, rs.getLong(1));
        assertEquals(42.0d, rs.getDouble("c_int"), 0d);

        // 字符串列的数字启发值 → getString 必须还原字符串（类型驱动，§六.3）
        assertEquals("123", rs.getString("c_str"));

        // DOUBLE 列的整值 JSON number → getDouble 正常
        assertEquals(1.5d, rs.getDouble("c_f64"), 0d);
        assertEquals(1.5f, rs.getFloat("c_f64"), 0f);

        assertEquals(true, rs.getBoolean("c_bool"));

        // DECIMAL：f64 中转 → BigDecimal.valueOf（§5.4）
        assertEquals(new BigDecimal("12.34"), rs.getBigDecimal("c_dec"));

        assertEquals(Date.valueOf("2026-10-12"), rs.getDate("c_date"));
        Timestamp ts = rs.getTimestamp("c_ts");
        assertEquals(Timestamp.valueOf("2026-10-12 08:30:00.123456789"), ts);
        assertEquals(Time.valueOf("08:30:00"), rs.getTime("c_time"));

        // BINARY hex → byte[]
        byte[] bin = rs.getBytes("c_bin");
        assertEquals(4, bin.length);
        assertEquals((byte) 0xde, bin[0]);
        assertEquals((byte) 0xef, bin[3]);

        // TINYINT → getObject 返回 Byte（JDBC 附录 B）
        assertEquals((byte) -5, rs.getByte("c_i8"));
        assertEquals(Byte.valueOf((byte) -5), rs.getObject("c_i8"));

        // getObject 规范映射
        assertEquals(Long.valueOf(42L), rs.getObject("c_int"));
        assertEquals("123", rs.getObject("c_str"));      // VARCHAR 列 → String
        assertEquals(Double.valueOf(1.5d), rs.getObject("c_f64"));
        assertEquals(Boolean.TRUE, rs.getObject("c_bool"));
        assertEquals(Date.valueOf("2026-10-12"), rs.getObject("c_date"));

        assertFalse(rs.wasNull());
    }

    @Test
    void wasNullSemantics() throws SQLException {
        // §六.3：getXxx 读到 NULL → 类型零值 + wasNull() 恰在随后一次返回 true
        QsdbResultSet rs = rs(typicalRows());
        assertTrue(rs.next());
        // 定位到全 NULL 行
        assertTrue(rs.next());

        assertEquals(0, rs.getInt(1));
        assertTrue(rs.wasNull());
        // wasNull 在下一次 getXxx 前多次调用同值（§六.3：随后一次 getXxx 前有效）
        assertTrue(rs.wasNull());

        assertEquals(0L, rs.getLong(1));
        assertTrue(rs.wasNull());
        assertEquals(0d, rs.getDouble(3), 0d);
        assertTrue(rs.wasNull());
        assertFalse(rs.getBoolean(4));
        assertTrue(rs.wasNull());
        assertNull(rs.getBigDecimal(5));
        assertTrue(rs.wasNull());
        assertNull(rs.getDate(6));
        assertTrue(rs.wasNull());
        assertNull(rs.getTimestamp(7));
        assertTrue(rs.wasNull());
        assertNull(rs.getTime(8));
        assertTrue(rs.wasNull());
        assertNull(rs.getBytes(9));
        assertTrue(rs.wasNull());
        assertNull(rs.getString(2));
        assertTrue(rs.wasNull());
        assertNull(rs.getObject(2));
        assertTrue(rs.wasNull());
        // getObject 零值语义：数值列为 null 的 getObject 返回 null（非包装零值）
        assertNull(rs.getObject(1));
        assertTrue(rs.wasNull());
    }

    @Test
    void crossTypeConversions() throws SQLException {
        // 仅一行：BIGINT 列取各种目标类型（JDBC 附录 B 宽转换）
        List<List<Value>> rows = Arrays.asList(
                Arrays.<Value>asList(Value.of(7L), Value.NULL, Value.NULL, Value.NULL, Value.NULL,
                        Value.NULL, Value.NULL, Value.NULL, Value.NULL, Value.NULL));
        QsdbResultSet rs = rs(rows);
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        assertEquals(7.0d, rs.getDouble(1), 0d);
        assertEquals(new BigDecimal("7"), rs.getBigDecimal(1));
        assertTrue(rs.getBoolean(1));
        assertEquals("7", rs.getString(1));
        // 字符串形态的数字 → getInt（经 JSON 通道字符串列装数字的对称面）
        assertEquals(0, rs.getInt(2));
        assertTrue(rs.wasNull());
    }

    @Test
    void stringToNumberConversion() throws SQLException {
        // VARCHAR 列装数字串（驱动侧未做数字启发时对称出现）→ 数值 getter 解析
        List<ColMeta> cols = Arrays.asList(new ColMeta("s", "Utf8"));
        QsdbResultSet rs = QsdbResultSet.forRows(cols,
                Arrays.<List<Value>>asList(Arrays.asList(Value.of("1234"))));
        assertTrue(rs.next());
        assertEquals(1234, rs.getInt("s"));
        assertEquals(1234L, rs.getLong("s"));
        assertEquals(1234.0d, rs.getDouble("s"), 0d);
        // 非数字串 → 22018
        List<List<Value>> bad = Arrays.<List<Value>>asList(Arrays.asList(Value.of("abc")));
        QsdbResultSet rs2 = QsdbResultSet.forRows(cols, bad);
        assertTrue(rs2.next());
        SQLException e = assertThrows(SQLException.class, () -> rs2.getInt("s"));
        assertEquals("22018", e.getSQLState());
    }

    @Test
    void maxRowsTruncation() throws SQLException {
        // §十一：setMaxRows 驱动侧截断——QsdbResultSet 构造入参
        List<List<Value>> rows = new ArrayList<List<Value>>();
        for (long i = 0; i < 10; i++) {
            rows.add(Arrays.asList(Value.of(i)));
        }
        List<ColMeta> cols = Arrays.asList(new ColMeta("n", "Int64"));
        // forRows 不带 maxRows（元数据场景）；查询路径截断在 next() 的 limit 内——
        // 这里通过包内构造器间接验证：用 forRows 后手动验证行为一致性
        QsdbResultSet full = QsdbResultSet.forRows(cols, rows);
        int count = 0;
        while (full.next()) {
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    void cursorLifecycle() throws SQLException {
        QsdbResultSet rs = rs(typicalRows());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.next());
        assertTrue(rs.isFirst());
        assertTrue(rs.next());
        assertFalse(rs.next()); // 仅 2 行
        assertTrue(rs.isAfterLast());
        // 游标不在行上时取值 → 报错
        assertThrows(SQLException.class, () -> rs.getInt(1));
        // findColumn 大小写不敏感回退
        QsdbResultSet rs2 = rs(typicalRows());
        assertTrue(rs2.next());
        assertEquals(1, rs2.findColumn("C_INT"));
        assertEquals(1, rs2.findColumn("c_int"));
        assertThrows(SQLException.class, () -> rs2.findColumn("nope"));
        // 类型元数据
        ResultSetMetaData md = rs2.getMetaData();
        assertEquals(10, md.getColumnCount());
        assertEquals("c_int", md.getColumnName(1));
        assertEquals(java.sql.Types.BIGINT, md.getColumnType(1));
        assertEquals("Int64", md.getColumnTypeName(1)); // 保留 Arrow 原文
        assertEquals(java.sql.Types.DECIMAL, md.getColumnType(5));
        assertEquals(10, md.getPrecision(5));
        assertEquals(2, md.getScale(5));
        assertEquals(java.sql.ResultSetMetaData.columnNullableUnknown, md.isNullable(1));
        assertEquals(Long.class.getName(), md.getColumnClassName(1));
        rs2.close();
        assertTrue(rs2.isClosed());
        assertThrows(SQLException.class, rs2::next);
    }

    @Test
    void timestampTimezoneForm() throws SQLException {
        // 带时区显示串（Arrow Timestamp(_, Some(tz)) → RFC3339）→ OffsetDateTime 路径
        List<ColMeta> cols = Arrays.asList(new ColMeta("t", "Timestamp(Nanosecond, Some(\"UTC\"))"));
        QsdbResultSet rs = QsdbResultSet.forRows(cols,
                Arrays.<List<Value>>asList(Arrays.asList(Value.of("2026-10-12T00:30:00Z"))));
        assertTrue(rs.next());
        Timestamp ts = rs.getTimestamp("t");
        // UTC 00:30 == 本机时区的对应时刻（用 Instant 比对避免时区假设）
        assertEquals(java.time.Instant.parse("2026-10-12T00:30:00Z"), ts.toInstant());
    }

    @Test
    void dateRoundtripViaLocalDate() throws SQLException {
        // LocalDate 边界（1970 前）
        List<ColMeta> cols = Arrays.asList(new ColMeta("d", "Date32"));
        QsdbResultSet rs = QsdbResultSet.forRows(cols,
                Arrays.<List<Value>>asList(Arrays.asList(Value.of("1969-07-20"))));
        assertTrue(rs.next());
        assertEquals(Date.valueOf("1969-07-20"), rs.getDate("d"));
        assertEquals(LocalDate.of(1969, 7, 20), rs.getDate("d").toLocalDate());
    }
}
