package cn.qinwh.qsdb.jdbc.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import cn.qinwh.qsdb.jdbc.QsdbException;

/**
 * 零依赖 JSON 解析器单测（含 §五.1 契约响应形态）。
 */
class MiniJsonTest {

    @SuppressWarnings("unchecked")
    private static Map<String, Object> obj(String json) throws QsdbException {
        return (Map<String, Object>) MiniJson.parse(json);
    }

    @Test
    void queryResponseShape() throws QsdbException {
        Map<String, Object> root = obj("{\"schema\":[{\"name\":\"id\",\"type\":\"Int64\"}],"
                + "\"rows\":[[1,\"a\",null,true,12.5]],\"rowCount\":1,\"elapsedMs\":3.2}");
        List<Object> schema = (List<Object>) root.get("schema");
        Map<String, Object> col = (Map<String, Object>) schema.get(0);
        assertEquals("id", col.get("name"));
        assertEquals("Int64", col.get("type"));
        List<Object> rows = (List<Object>) root.get("rows");
        List<Object> row = (List<Object>) rows.get(0);
        assertEquals(1L, row.get(0));
        assertEquals("a", row.get(1));
        assertNull(row.get(2));
        assertEquals(Boolean.TRUE, row.get(3));
        assertEquals(12.5, row.get(4));
    }

    @Test
    void scalars() throws QsdbException {
        assertEquals("hello", MiniJson.parse("\"hello\""));
        assertEquals(42L, MiniJson.parse("42"));
        assertEquals(-7L, MiniJson.parse("-7"));
        assertEquals(1.5, MiniJson.parse("1.5"));
        assertEquals(1e10, ((Double) MiniJson.parse("1e10")), 1e-6);
        assertEquals(Boolean.TRUE, MiniJson.parse("true"));
        assertEquals(Boolean.FALSE, MiniJson.parse("false"));
        assertNull(MiniJson.parse("null"));
    }

    @Test
    void escapes() throws QsdbException {
        assertEquals("a\"b\\c\nd\te\u4e2d", MiniJson.parse("\"a\\\"b\\\\c\\nd\\te\\u4e2d\""));
        // SQL 注入串原样可解析
        assertEquals("'; DROP TABLE t; --", MiniJson.parse("\"'; DROP TABLE t; --\""));
    }

    @Test
    void nestedArraysAndObjects() throws QsdbException {
        Map<String, Object> root = obj("{\"a\":[1,[2,3],{\"b\":null}],\"c\":{}}");
        List<Object> a = (List<Object>) root.get("a");
        assertEquals(3, a.size());
        assertEquals(2L, ((List<Object>) a.get(1)).get(0));
    }

    @Test
    void malformedRejected() {
        assertThrows(QsdbException.class, () -> MiniJson.parse("{"));
        assertThrows(QsdbException.class, () -> MiniJson.parse("[1,]"));
        assertThrows(QsdbException.class, () -> MiniJson.parse("\"unclosed"));
        assertThrows(QsdbException.class, () -> MiniJson.parse("{\"a\" 1}"));
        assertThrows(QsdbException.class, () -> MiniJson.parse("nul"));
        assertThrows(QsdbException.class, () -> MiniJson.parse("1 2"));
    }

    @Test
    void writeStringEscaping() {
        StringBuilder sb = new StringBuilder();
        MiniJson.writeString(sb, "a\"b\\c\nd");
        assertEquals("\"a\\\"b\\\\c\\nd\"", sb.toString());
        StringBuilder sb2 = new StringBuilder();
        MiniJson.writeString(sb2, "'; DROP TABLE t; --");
        assertEquals("\"'; DROP TABLE t; --\"", sb2.toString());
        StringBuilder sb3 = new StringBuilder();
        MiniJson.writeString(sb3, String.valueOf((char) 1));
        assertEquals("\"\\u0001\"", sb3.toString());
    }
}
