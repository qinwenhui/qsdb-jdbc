package cn.qinwh.qsdb.jdbc.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * ParamRenderer 全规则单测（设计文档 §七.2 + M1 注入样本集验收项）。
 */
class ParamRendererTest {

    private static String render(String sql, Object... values) throws SQLException {
        ParamRenderer.Param[] params = new ParamRenderer.Param[values.length];
        for (int i = 0; i < values.length; i++) {
            Object v = values[i];
            if (v == null) {
                params[i] = ParamRenderer.Param.nullValue(QsdbType.VARCHAR);
            } else if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
                params[i] = ParamRenderer.Param.of(QsdbType.BIGINT, v);
            } else if (v instanceof Float) {
                params[i] = ParamRenderer.Param.of(QsdbType.FLOAT, v);
            } else if (v instanceof Double) {
                params[i] = ParamRenderer.Param.of(QsdbType.DOUBLE, v);
            } else if (v instanceof BigDecimal) {
                params[i] = ParamRenderer.Param.of(QsdbType.DECIMAL, v);
            } else if (v instanceof Boolean) {
                params[i] = ParamRenderer.Param.of(QsdbType.BOOLEAN, v);
            } else if (v instanceof String) {
                params[i] = ParamRenderer.Param.of(QsdbType.VARCHAR, v);
            } else if (v instanceof Date) {
                params[i] = ParamRenderer.Param.of(QsdbType.DATE, v);
            } else if (v instanceof Time) {
                params[i] = ParamRenderer.Param.of(QsdbType.TIME, v);
            } else if (v instanceof Timestamp) {
                params[i] = ParamRenderer.Param.of(QsdbType.TIMESTAMP, v);
            } else {
                throw new IllegalArgumentException(v.getClass().getName());
            }
        }
        return ParamRenderer.render(sql, params);
    }

    // ------------------------------------------------------------------
    // 占位符计数（§七.2.1：字符串/注释/标识符内的 ? 不计）
    // ------------------------------------------------------------------

    @Test
    void countBasics() throws SQLException {
        assertEquals(0, ParamRenderer.countPlaceholders("SELECT 1"));
        assertEquals(1, ParamRenderer.countPlaceholders("SELECT ?"));
        assertEquals(3, ParamRenderer.countPlaceholders("SELECT ?, ?, ?"));
    }

    @Test
    void placeholdersInsideStringLiteralNotCounted() throws SQLException {
        assertEquals(0, ParamRenderer.countPlaceholders("SELECT 'hello ? world'"));
        assertEquals(1, ParamRenderer.countPlaceholders("SELECT ?, 'hello ? world'"));
        // '' 转义引号内的 ?
        assertEquals(0, ParamRenderer.countPlaceholders("SELECT 'it''s ? here'"));
        assertEquals(1, ParamRenderer.countPlaceholders("SELECT 'a''b ? c''d', ?"));
    }

    @Test
    void placeholdersInsideCommentsNotCounted() throws SQLException {
        assertEquals(0, ParamRenderer.countPlaceholders("SELECT 1 -- comment ? here"));
        assertEquals(0, ParamRenderer.countPlaceholders("SELECT /* block ? comment */ 1"));
        assertEquals(1, ParamRenderer.countPlaceholders("SELECT ? -- trailing ?"));
        assertEquals(2, ParamRenderer.countPlaceholders("SELECT ?, /* mid ? */ ?"));
    }

    @Test
    void placeholdersInsideQuotedIdentifiersNotCounted() throws SQLException {
        assertEquals(0, ParamRenderer.countPlaceholders("SELECT \"col?\" FROM t"));
        assertEquals(0, ParamRenderer.countPlaceholders("SELECT `col?` FROM t"));
        assertEquals(1, ParamRenderer.countPlaceholders("SELECT \"col?\", ? FROM t"));
    }

    @Test
    void malformedSqlRejected() {
        assertThrows(SQLException.class, () -> ParamRenderer.countPlaceholders("SELECT 'unclosed"));
        assertThrows(SQLException.class, () -> ParamRenderer.countPlaceholders("SELECT /* unclosed"));
        assertThrows(SQLException.class, () -> ParamRenderer.countPlaceholders("SELECT \"unclosed"));
    }

    // ------------------------------------------------------------------
    // 类型化字面量渲染（§6.2 形态表）
    // ------------------------------------------------------------------

    @Test
    void renderTypedLiterals() throws SQLException {
        assertEquals("SELECT 42", render("SELECT ?", 42));
        assertEquals("SELECT -7", render("SELECT ?", -7));
        assertEquals("SELECT TRUE", render("SELECT ?", true));
        assertEquals("SELECT FALSE", render("SELECT ?", false));
        assertEquals("SELECT 12.5", render("SELECT ?", 12.5d));
        assertEquals("SELECT 0.25", render("SELECT ?", 0.25f));
        assertEquals("SELECT 123.45", render("SELECT ?", new BigDecimal("123.45")));
        assertEquals("SELECT NULL", render("SELECT ?", (Object) null));
    }

    @Test
    void renderTemporalLiterals() throws SQLException {
        assertEquals("SELECT DATE '2026-10-12'",
                render("SELECT ?", Date.valueOf("2026-10-12")));
        assertEquals("SELECT TIME '08:30:00'",
                render("SELECT ?", Time.valueOf("08:30:00")));
        // 带纳秒的时间戳：空格分隔（内核 parse_timestamp_ns 接受），去尾零
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2026, 10, 12, 8, 30, 0, 123_456_000));
        assertEquals("SELECT TIMESTAMP '2026-10-12 08:30:00.123456'", render("SELECT ?", ts));
        // 整秒不带小数
        Timestamp whole = Timestamp.valueOf(LocalDateTime.of(2026, 1, 2, 3, 4, 5, 0));
        assertEquals("SELECT TIMESTAMP '2026-01-02 03:04:05'", render("SELECT ?", whole));
    }

    // ------------------------------------------------------------------
    // 注入样本集（M1 验收：字符串转义 / 注释吞占位符）
    // ------------------------------------------------------------------

    @Test
    void injectionSamplesAreNeutralized() throws SQLException {
        // 经典注入串：单引号必须翻倍，否则突破字符串边界
        String evil = "'; DROP TABLE users; --";
        String rendered = render("SELECT * FROM t WHERE name = ?", evil);
        assertEquals("SELECT * FROM t WHERE name = '''; DROP TABLE users; --'",
                rendered);
        // 整体仍是一个字符串字面量：参数个数 1，渲染后占位符消失
        assertEquals(0, ParamRenderer.countPlaceholders(rendered));

        // 转义引号注入
        assertEquals("SELECT 'a''''b'", render("SELECT ?", "a''b"));

        // 反斜杠不被特殊处理（QSDB 字符串字面量不识别反斜杠转义——与内核一致）
        // 值 a\' → 'a\'''（仅 '' 转义生效，反斜杠原样）
        assertEquals("SELECT 'a\\'''", render("SELECT ?", "a\\'"));

        // Unicode 与普通内容原样
        assertEquals("SELECT '中文✓'", render("SELECT ?", "中文✓"));
    }

    @Test
    void controlCharactersRejected() throws SQLException {
        // 控制字符（<0x20）渲染为拒绝——不猜测内核行为（§七.2.3）
        SQLException e = assertThrows(SQLException.class,
                () -> render("SELECT ?", "bad\nstring"));
        assertEquals("22018", ((SQLException) e).getSQLState());
        assertThrows(SQLException.class, () -> render("SELECT ?", "tab\there"));
        assertThrows(SQLException.class, () -> render("SELECT ?", "x" + (char) 0)); // NUL
        assertThrows(SQLException.class, () -> render("SELECT ?", "x" + (char) 1)); // SOH
        // 0x20 本身是合法空格，不拒绝
        assertEquals("SELECT 'a b'", render("SELECT ?", "a b"));
    }

    @Test
    void nanInfinityRejected() {
        SQLException e = assertThrows(SQLException.class, () -> render("SELECT ?", Double.NaN));
        assertTrue(e.getMessage().contains("NaN"));
        assertThrows(SQLException.class, () -> render("SELECT ?", Double.POSITIVE_INFINITY));
        assertThrows(SQLException.class, () -> render("SELECT ?", Float.NEGATIVE_INFINITY));
    }

    // ------------------------------------------------------------------
    // 参数校验（fail-fast，§七.2.1）
    // ------------------------------------------------------------------

    @Test
    void parameterCountMismatchFailsFast() {
        SQLException tooFew = assertThrows(SQLException.class, () -> render("SELECT ?, ?", 1));
        assertTrue(tooFew.getMessage().contains("mismatch"));
        assertThrows(SQLException.class, () -> render("SELECT ?", 1, 2));
    }

    @Test
    void unsetParameterRejected() {
        ParamRenderer.Param[] params = new ParamRenderer.Param[1];
        SQLException e = assertThrows(SQLException.class,
                () -> ParamRenderer.render("SELECT ?", params));
        assertTrue(e.getMessage().contains("not set"));
    }

    @Test
    void multiplyRenderIsStable() throws SQLException {
        // 同一语句可重复渲染（PreparedStatement 重执行路径）
        String sql = "SELECT ?, ?";
        assertEquals("SELECT 1, 'x'", render(sql, 1, "x"));
        assertEquals("SELECT 1, 'x'", render(sql, 1, "x"));
    }
}
