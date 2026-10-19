package cn.qinwh.qsdb.jdbc.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * 两种类型字符串方言的归一单测（§五.3；样例形态已按内核代码核实）。
 */
class TypeStringsTest {

    private static void arrow(String raw, QsdbType expected) throws SQLException {
        assertEquals(expected, TypeStrings.parse(raw).type, raw);
    }

    private static void logical(String raw, QsdbType expected) throws SQLException {
        assertEquals(expected, TypeStrings.parse(raw).type, raw);
    }

    // ---- Arrow 方言（query 响应 schema[].type = DataType::to_string()）----

    @Test
    void arrowDialect() throws SQLException {
        arrow("Int8", QsdbType.TINYINT);
        arrow("Int16", QsdbType.SMALLINT);
        arrow("Int32", QsdbType.INT);
        arrow("Int64", QsdbType.BIGINT);
        arrow("Float32", QsdbType.FLOAT);
        arrow("Float64", QsdbType.DOUBLE);
        arrow("Utf8", QsdbType.VARCHAR);
        arrow("LargeUtf8", QsdbType.VARCHAR);
        arrow("Boolean", QsdbType.BOOLEAN);
        arrow("Date32", QsdbType.DATE);
        arrow("Date64", QsdbType.DATE);
        arrow("Binary", QsdbType.BINARY);
        arrow("Time64(Nanosecond)", QsdbType.TIME);
        arrow("Timestamp(Nanosecond, None)", QsdbType.TIMESTAMP);
        arrow("Timestamp(Nanosecond, Some(\"UTC\"))", QsdbType.TIMESTAMP);
        arrow("Interval(MonthDayNano)", QsdbType.INTERVAL);
    }

    // ---- LogicalType 方言（/tables/{t}/schema = Display）----

    @Test
    void logicalDialect() throws SQLException {
        logical("BOOLEAN", QsdbType.BOOLEAN);
        logical("TINYINT", QsdbType.TINYINT);
        logical("SMALLINT", QsdbType.SMALLINT);
        logical("INT", QsdbType.INT);
        logical("INTEGER", QsdbType.INT);
        logical("BIGINT", QsdbType.BIGINT);
        logical("FLOAT", QsdbType.FLOAT);
        logical("DOUBLE", QsdbType.DOUBLE);
        logical("STRING", QsdbType.VARCHAR); // 内核 Varchar{max_chars:None} 的 Display
        logical("VARCHAR(255)", QsdbType.VARCHAR);
        logical("CHAR(10)", QsdbType.VARCHAR);
        logical("BINARY", QsdbType.BINARY);
        logical("DATE", QsdbType.DATE);
        logical("TIME", QsdbType.TIME);
        logical("TIMESTAMP", QsdbType.TIMESTAMP);
        logical("TIMESTAMP WITH TIME ZONE (UTC)", QsdbType.TIMESTAMP);
        logical("INTERVAL", QsdbType.INTERVAL);
    }

    // ---- 精度/标度提取 ----

    @Test
    void decimalPrecisionScale() throws SQLException {
        // 内核 DecimalMeta Display：DECIMAL(10, 2)——逗号后带空格
        TypeStrings.Parsed p = TypeStrings.parse("DECIMAL(10, 2)");
        assertEquals(QsdbType.DECIMAL, p.type);
        assertEquals(10, p.precision);
        assertEquals(2, p.scale);
        // Arrow 形态
        TypeStrings.Parsed pa = TypeStrings.parse("Decimal128(38, 10)");
        assertEquals(38, pa.precision);
        assertEquals(10, pa.scale);
    }

    @Test
    void varcharAndCharPrecision() throws SQLException {
        assertEquals(255, TypeStrings.parse("VARCHAR(255)").precision);
        assertEquals(10, TypeStrings.parse("CHAR(10)").precision);
        assertEquals(QsdbType.VARCHAR.getDefaultPrecision(), TypeStrings.parse("STRING").precision);
    }

    @Test
    void caseAndWhitespaceTolerant() throws SQLException {
        assertEquals(QsdbType.BIGINT, TypeStrings.parse("bigint").type);
        assertEquals(QsdbType.BIGINT, TypeStrings.parse("  Int64  ").type);
        assertEquals(QsdbType.TIMESTAMP, TypeStrings.parse("timestamp(nanosecond, none)").type);
    }

    @Test
    void unknownTypeThrows() {
        SQLException e = assertThrows(SQLException.class, () -> TypeStrings.parse("Map(Int64, Utf8)"));
        assertEquals("HY000", e.getSQLState());
        assertThrows(SQLException.class, () -> TypeStrings.parse("List(Utf8)"));
    }

    @Test
    void nullOrEmptyDefaultsToVarchar() throws SQLException {
        assertEquals(QsdbType.VARCHAR, TypeStrings.parse(null).type);
        assertEquals(QsdbType.VARCHAR, TypeStrings.parse("  ").type);
    }
}
