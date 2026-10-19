package cn.qinwh.qsdb.jdbc.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * 批量合并器单测。**词法边界是本类的主要风险**——值里含逗号/引号/括号/
 * 关键字时绝不能误切（错切 = 写错数据，比不合并严重得多）。
 */
class BatchMergeTest {

    // ---- 元组计数与词法边界 ----

    @Test
    void countSimpleTuples() {
        assertEquals(1, BatchMerge.countTopLevelTuples(" (1, 2)"));
        assertEquals(2, BatchMerge.countTopLevelTuples(" (1, 2), (3, 4)"));
        assertEquals(3, BatchMerge.countTopLevelTuples("(1),(2),(3)"));
        assertEquals(1, BatchMerge.countTopLevelTuples(" (1)")); // 单列
        assertEquals(2, BatchMerge.countTopLevelTuples(" (1), (2);")); // 尾随分号
    }

    @Test
    void commasInsideNestedParensAndLiteralsAreNotSeparators() {
        // 函数调用里的逗号
        assertEquals(1, BatchMerge.countTopLevelTuples(" (f(1, 2), 3)"));
        assertEquals(2, BatchMerge.countTopLevelTuples(" (f(1,2)), (g(3,4))"));
        // 字符串里的逗号、括号、分号
        assertEquals(1, BatchMerge.countTopLevelTuples(" ('a, b', 'c)d', 'e;f')"));
        assertEquals(2, BatchMerge.countTopLevelTuples(" ('x,y'), ('z(w)')"));
        // '' 转义引号
        assertEquals(1, BatchMerge.countTopLevelTuples(" ('it''s , ok')"));
        // 字符串里含 VALUES 关键字
        assertEquals(1, BatchMerge.countTopLevelTuples(" ('VALUES (9,9)')"));
    }

    @Test
    void malformedShapesRejected() {
        assertEquals(-1, BatchMerge.countTopLevelTuples(" 1, 2"));       // 无括号
        assertEquals(-1, BatchMerge.countTopLevelTuples(" (1, 2"));      // 不配平
        assertEquals(-1, BatchMerge.countTopLevelTuples(" (1), x"));     // 尾随子句
        assertEquals(-1, BatchMerge.countTopLevelTuples(" (1) ON CONFLICT DO NOTHING"));
        assertEquals(-1, BatchMerge.countTopLevelTuples(""));
        assertEquals(-1, BatchMerge.countTopLevelTuples(null));
    }

    // ---- VALUES 子句定位 ----

    @Test
    void findValuesClause() {
        assertTrue(BatchMerge.findValuesClause("INSERT INTO t VALUES (1)") > 0);
        assertTrue(BatchMerge.findValuesClause("insert into t values (1)") > 0);
        // 表名恰好叫 values：必须命中第二个（其后跟 '('）
        String tricky = "INSERT INTO values VALUES (1)";
        int idx = BatchMerge.findValuesClause(tricky);
        assertEquals(tricky.indexOf("VALUES", 15), idx);
        // 字符串里的 VALUES 不算
        assertEquals(-1, BatchMerge.findValuesClause("SELECT 'VALUES (1)' FROM t"));
        // 注释里的不算
        assertEquals(-1, BatchMerge.findValuesClause("SELECT 1 -- VALUES (1)\n"));
        // 非 INSERT（无 VALUES 子句）
        assertEquals(-1, BatchMerge.findValuesClause("SELECT * FROM t"));
        assertEquals(-1, BatchMerge.findValuesClause("INSERT INTO t SELECT * FROM s"));
    }

    // ---- 原始 SQL 合并 ----

    @Test
    void mergeRawStatements() {
        List<String> batch = Arrays.asList(
                "INSERT INTO t VALUES (1, 'a')",
                "INSERT INTO t VALUES (2, 'b')",
                "INSERT INTO t VALUES (3, 'c')");
        List<BatchMerge.Group> groups = BatchMerge.group(BatchMerge.fragmentsOf(batch));
        assertEquals(1, groups.size());
        assertTrue(groups.get(0).isMerged());
        assertEquals("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')", groups.get(0).sql);
        assertEquals(3, groups.get(0).entryCount);
        assertEquals(1, groups.get(0).counts[0]);
        assertEquals(1, groups.get(0).counts[2]);
    }

    @Test
    void multiRowEntriesKeepPerEntryCounts() {
        List<String> batch = Arrays.asList(
                "INSERT INTO t VALUES (1), (2)",
                "INSERT INTO t VALUES (3)");
        List<BatchMerge.Group> groups = BatchMerge.group(BatchMerge.fragmentsOf(batch));
        assertEquals(1, groups.size());
        assertEquals(2, groups.get(0).counts[0]); // 第一条贡献 2 行
        assertEquals(1, groups.get(0).counts[1]);
        assertEquals("INSERT INTO t VALUES (1), (2), (3)", groups.get(0).sql);
    }

    @Test
    void differentPrefixesAreNotMerged() {
        List<String> batch = Arrays.asList(
                "INSERT INTO t VALUES (1)",
                "INSERT INTO other VALUES (2)");
        List<BatchMerge.Group> groups = BatchMerge.group(BatchMerge.fragmentsOf(batch));
        assertEquals(2, groups.size());
        assertFalse(groups.get(0).isMerged());
        assertEquals("INSERT INTO other VALUES (2)", groups.get(1).sql);
    }

    @Test
    void opaqueEntriesExecuteSeparately() {
        List<String> batch = Arrays.asList(
                "INSERT INTO t VALUES (1)",
                "CREATE TABLE x (a INT)",
                "INSERT INTO t VALUES (2)");
        List<BatchMerge.Group> groups = BatchMerge.group(BatchMerge.fragmentsOf(batch));
        assertEquals(3, groups.size());
        assertEquals(1, groups.get(0).entryCount);           // 前后被 DDL 隔断
        assertNull(groups.get(1).counts);                    // 不透明：行数执行后取
        assertEquals("CREATE TABLE x (a INT)", groups.get(1).sql);
        assertEquals(1, groups.get(2).entryCount);
    }

    @Test
    void chunkingHonoursRowCap() {
        List<String> batch = new java.util.ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            batch.add("INSERT INTO t VALUES (" + i + ")");
        }
        // 行数上限 4 → 10 条切成 4+4+2 三组
        List<BatchMerge.Group> groups =
                BatchMerge.group(BatchMerge.fragmentsOf(batch), 4, Integer.MAX_VALUE);
        assertEquals(3, groups.size());
        assertEquals(4, groups.get(0).entryCount);
        assertEquals(4, groups.get(1).entryCount);
        assertEquals(2, groups.get(2).entryCount);
        // 覆盖全部条目且不重不漏
        int covered = 0;
        for (BatchMerge.Group g : groups) {
            assertEquals(covered, g.entryStart);
            covered += g.entryCount;
        }
        assertEquals(10, covered);
    }

    @Test
    void chunkingHonoursByteCap() {
        List<String> batch = Arrays.asList(
                "INSERT INTO t VALUES ('aaaaaaaaaa')",
                "INSERT INTO t VALUES ('bbbbbbbbbb')",
                "INSERT INTO t VALUES ('cccccccccc')");
        List<BatchMerge.Group> groups =
                BatchMerge.group(BatchMerge.fragmentsOf(batch), Integer.MAX_VALUE, 50);
        assertTrue(groups.size() > 1, "字节上限应触发分组");
    }

    @Test
    void mergeIsLexicallyFaithfulForTrickyValues() {
        // 值里含逗号/括号/引号/分号/关键字——合并后必须逐字保真
        List<String> batch = Arrays.asList(
                "INSERT INTO t VALUES ('a,b', 'c(d)', 'e;f')",
                "INSERT INTO t VALUES ('it''s', 'VALUES (9)', 'g')");
        List<BatchMerge.Group> groups = BatchMerge.group(BatchMerge.fragmentsOf(batch));
        assertEquals(1, groups.size());
        assertEquals("INSERT INTO t VALUES ('a,b', 'c(d)', 'e;f'), ('it''s', 'VALUES (9)', 'g')",
                groups.get(0).sql);
        assertEquals(1, groups.get(0).counts[0]);
        assertEquals(1, groups.get(0).counts[1]);
    }

    // ---- 预处理模板拆分 ----

    @Test
    void splitPreparedTemplate() {
        BatchMerge.Template t = BatchMerge.splitTemplate("INSERT INTO t VALUES (?, ?, ?)", 3);
        assertEquals("INSERT INTO t VALUES", t.prefix);
        assertEquals(1, BatchMerge.countTopLevelTuples(t.tupleTemplate));
        // 带列清单
        BatchMerge.Template t2 =
                BatchMerge.splitTemplate("INSERT INTO t (a, b) VALUES (?, ?)", 2);
        assertEquals("INSERT INTO t (a, b) VALUES", t2.prefix);
    }

    @Test
    void splitRejectsUnmergeableTemplates() {
        // 占位符在 VALUES 之前（前缀含 ?）→ 无法按元组渲染
        assertNull(BatchMerge.splitTemplate(
                "INSERT INTO t SELECT ? WHERE x = 1", 1));
        // 无 VALUES 子句
        assertNull(BatchMerge.splitTemplate("SELECT ? FROM t", 1));
        // 占位符数不符
        assertNull(BatchMerge.splitTemplate("INSERT INTO t VALUES (?)", 3));
    }

    @Test
    void renderedTupleFragments() {
        BatchMerge.Template t = BatchMerge.splitTemplate("INSERT INTO t VALUES (?, ?)", 2);
        BatchMerge.Fragment f =
                BatchMerge.fragmentOfRenderedTuple(t.prefix, " (1, 'a,b')");
        assertTrue(f.isMergeable());
        assertEquals(1, f.tupleCount);
        assertFalse(BatchMerge.fragmentOfRenderedTuple(t.prefix, " broken").isMergeable());
    }
}
