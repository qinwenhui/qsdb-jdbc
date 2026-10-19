package cn.qinwh.qsdb.jdbc.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.jupiter.api.Test;

import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * 服务端参数通道编码单测（对齐内核 {@code crates/qsdb/src/sql/params.rs} 的
 * {@code json_to_literal} / {@code parse_literal_text} 接受面，P1 2026-10-19）。
 */
class ParamJsonTest {

    private static String of(QsdbType t, Object v) throws SQLException {
        return ParamJson.toJsonArray(new ParamRenderer.Param[] { ParamRenderer.Param.of(t, v) });
    }

    @Test
    void emptyParamsOmitField() throws SQLException {
        assertNull(ParamJson.toJsonArray(null), "无参数 → null（不发 params 字段）");
        assertNull(ParamJson.toJsonArray(new ParamRenderer.Param[0]));
    }

    @Test
    void bareForms() throws SQLException {
        assertEquals("[null]", of(QsdbType.BIGINT, null), "setNull → JSON null");
        assertEquals("[42]", of(QsdbType.BIGINT, 42L), "整数裸值（内核推 Int64）");
        assertEquals("[true]", of(QsdbType.BOOLEAN, Boolean.TRUE));
        assertEquals("[\"it's\"]", of(QsdbType.VARCHAR, "it's"), "字符串裸值");
        assertEquals("[2.75]", of(QsdbType.DOUBLE, 2.75d), "有限浮点裸值");
    }

    @Test
    void controlCharStringIsLegalOnServerChannel() throws SQLException {
        // 文本渲染通道拒绝控制字符；server 通道经 JSON 转义（backslash-u）合法
        String s = "line1\nline2\tend";
        assertEquals("[\"line1\\nline2\\tend\"]", of(QsdbType.VARCHAR, s));
    }

    @Test
    void nonFiniteFloatsViaTypedStringForm() throws SQLException {
        // JSON 无 NaN/Infinity number 形态 → 显式类型 + 字符串（Rust f64 parse 接受）
        assertEquals("[{\"type\":\"DOUBLE\",\"value\":\"NaN\"}]",
                of(QsdbType.DOUBLE, Double.NaN));
        assertEquals("[{\"type\":\"DOUBLE\",\"value\":\"Infinity\"}]",
                of(QsdbType.DOUBLE, Double.POSITIVE_INFINITY));
        assertEquals("[{\"type\":\"DOUBLE\",\"value\":\"-Infinity\"}]",
                of(QsdbType.DOUBLE, Double.NEGATIVE_INFINITY));
        assertEquals("[{\"type\":\"FLOAT\",\"value\":\"NaN\"}]",
                of(QsdbType.FLOAT, Float.NaN));
    }

    @Test
    void decimalIsAlwaysTypedStringNeverBareNumber() throws SQLException {
        // 裸 JSON number 会经 f64 丢精度——DECIMAL 必须显式 + 字符串形态
        assertEquals("[{\"type\":\"DECIMAL(38,2)\",\"value\":\"12.34\"}]",
                of(QsdbType.DECIMAL, new BigDecimal("12.34")));
        // 高精度：形态保持逐字符（内核按字符串精确转换）
        assertEquals("[{\"type\":\"DECIMAL(38,20)\",\"value\":\"1.12345678901234567890\"}]",
                of(QsdbType.DECIMAL, new BigDecimal("1.12345678901234567890")));
        // scale<0（如 1E+3）展开为整数形态
        assertEquals("[{\"type\":\"DECIMAL(38,0)\",\"value\":\"1000\"}]",
                of(QsdbType.DECIMAL, new BigDecimal("1E+3")));
    }

    @Test
    void temporalTypesAreTyped() throws SQLException {
        // 裸字符串会被内核推成 VARCHAR——日期/时间必须显式
        assertEquals("[{\"type\":\"DATE\",\"value\":\"2026-01-02\"}]",
                of(QsdbType.DATE, Date.valueOf("2026-01-02")));
        assertEquals("[{\"type\":\"TIME\",\"value\":\"08:30:00\"}]",
                of(QsdbType.TIME, Time.valueOf("08:30:00")));
        assertEquals("[{\"type\":\"TIME\",\"value\":\"08:30:00.5\"}]",
                of(QsdbType.TIME, java.time.LocalTime.of(8, 30, 0, 500_000_000)),
                "LocalTime 原生携带纳秒（Time.valueOf 会静默丢）");
        assertEquals("[{\"type\":\"TIMESTAMP\",\"value\":\"2026-01-02 03:04:05.123456789\"}]",
                of(QsdbType.TIMESTAMP, Timestamp.valueOf("2026-01-02 03:04:05.123456789")));
    }

    @Test
    void binaryIsTypedHex() throws SQLException {
        assertEquals("[{\"type\":\"BINARY\",\"value\":\"6162\"}]",
                of(QsdbType.BINARY, new byte[] {0x61, 0x62}), "小写 hex（内核可逆契约）");
        assertEquals("[null]", of(QsdbType.BINARY, null));
    }

    @Test
    void intervalHasNoParameterForm() {
        // 内核 parse_literal_text 拒绝 INTERVAL——两通道一致显式报错
        assertThrows(SQLException.class,
                () -> of(QsdbType.INTERVAL, "1 day"));
    }

    @Test
    void textChannelRendersBinaryAsHexLiteral() throws SQLException {
        // 内核第九轮（5dfc928）起 X'…' 字面量可用——text 通道的 BINARY 转正
        ParamRenderer.Param p = ParamRenderer.Param.of(QsdbType.BINARY, new byte[] {0x61, 0x62});
        assertEquals("X'6162'", p.renderLiteral());
    }
}
