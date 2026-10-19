package cn.qinwh.qsdb.jdbc.internal;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量 INSERT 合并器：把「逐条 VALUES 语句」折叠为「单条多行 VALUES 语句」。
 *
 * <p><b>为什么</b>：内核 SQL 通道每条语句一次 append + manifest 提交
 * （~17ms/条），逐条批量时提交开销完全压过数据本身。实测（2026-10-13）：
 * <ul>
 *   <li>2000 行：逐条 34.3s → 合并 0.07s（~490×）</li>
 *   <li>100 万行：合并后驱动 8.7s（115k 行/秒），已触达裸 HTTP 的
 *       130k 行/秒 天花板——客户端开销基本消除</li>
 * </ul>
 * 剩余瓶颈在内核侧 SQL 文本的逐行解析绑定（~7.5µs/行）：百万行以上规模
 * 唯一出路是二进制 IPC 通道（/api/v1/write，驱动 roadmap 阶段 2）。
 *
 * <p><b>安全边界</b>：本类只做<b>结构识别与文本拼接</b>，从不生成字面量
 * ——字面量全部来自 {@link ParamRenderer}（PreparedStatement 路径）或
 * 调用方提供的原始 SQL（Statement 路径）。任何形态识别失败都<b>回退</b>为
 * 不合并（{@link #Fragments#opaque}），绝不猜测。故本类不是安全关键路径，
 * 但仍遵循与 ParamRenderer 相同的词法规则（字符串/注释/引用标识符）。
 *
 * <p>internal API——不在驱动公开面上。
 */
public final class BatchMerge {

    /** 单条合并语句的行数上限（与内核 512k 攒批阈值同向，取实测甜点区）。 */
    public static final int MAX_ROWS_PER_STATEMENT = 65536;

    /** 单条合并语句的文本字节上限（防宽表把请求体撑爆）。 */
    public static final int MAX_BYTES_PER_STATEMENT = 4 * 1024 * 1024;

    private BatchMerge() {
    }

    /**
     * 一个批条目的可合并形态：{@code prefix + tuples}。
     * {@code prefix} 为 null 表示「不可合并」（不透明条目，原样单独执行）。
     */
    public static final class Fragment {
        /** 字面量之前的部分，如 {@code INSERT INTO t VALUES}；null = 不透明。 */
        public final String prefix;
        /** 元组文本，如 {@code " (1, 'a'), (2, 'b')"}；不透明时为 null。 */
        public final String tuples;
        /** 该条目贡献的行数（顶层元组个数）；不透明时为 0。 */
        public final int tupleCount;
        /** 不透明条目的原始 SQL（用于单独执行）。 */
        public final String raw;

        private Fragment(String prefix, String tuples, int tupleCount, String raw) {
            this.prefix = prefix;
            this.tuples = tuples;
            this.tupleCount = tupleCount;
            this.raw = raw;
        }

        static Fragment mergeable(String prefix, String tuples, int tupleCount) {
            return new Fragment(prefix, tuples, tupleCount, null);
        }

        static Fragment opaque(String raw) {
            return new Fragment(null, null, 0, raw);
        }

        public boolean isMergeable() {
            return prefix != null;
        }
    }

    /** 一个执行组：要执行的语句 + 它覆盖的原批条目范围。 */
    public static final class Group {
        /** 实际下发的语句。 */
        public final String sql;
        /** 覆盖的起始条目下标。 */
        public final int entryStart;
        /** 覆盖的条目数。 */
        public final int entryCount;
        /** 逐条目行数；不透明条目为 null（行数执行后才有）。 */
        public final int[] counts;

        Group(String sql, int entryStart, int entryCount, int[] counts) {
            this.sql = sql;
            this.entryStart = entryStart;
            this.entryCount = entryCount;
            this.counts = counts;
        }

        /** 是否为合并结果（覆盖多个条目）。 */
        public boolean isMerged() {
            return entryCount > 1;
        }
    }

    // ------------------------------------------------------------------
    // 原始 SQL 路径（Statement.addBatch）
    // ------------------------------------------------------------------

    /**
     * 把一批原始 SQL 逐个识别为可合并片段（识别失败 → 不透明片段）。
     *
     * @return 与入参等长的片段列表（顺序一致）
     */
    public static List<Fragment> fragmentsOf(List<String> statements) {
        List<Fragment> out = new ArrayList<Fragment>(statements.size());
        for (String sql : statements) {
            out.add(fragmentOf(sql));
        }
        return out;
    }

    /** 识别单条原始 SQL：{@code INSERT … VALUES (…)[,…]} → 可合并片段。 */
    static Fragment fragmentOf(String sql) {
        if (sql == null) {
            return Fragment.opaque(null);
        }
        int idx = findValuesClause(sql);
        if (idx < 0) {
            return Fragment.opaque(sql);
        }
        String prefix = sql.substring(0, idx + VALUES_LEN).trim();
        String tuples = sql.substring(idx + VALUES_LEN);
        int count = countTopLevelTuples(tuples);
        if (count < 0) {
            return Fragment.opaque(sql);
        }
        return Fragment.mergeable(prefix, normalize(tuples), count);
    }

    /**
     * 分组：相邻的可合并片段在「同前缀」且「未超行数/字节上限」时合并为一组；
     * 不透明片段各自成组（原样执行）。
     */
    public static List<Group> group(List<Fragment> fragments) {
        return group(fragments, MAX_ROWS_PER_STATEMENT, MAX_BYTES_PER_STATEMENT);
    }

    public static List<Group> group(List<Fragment> fragments, int maxRows, int maxBytes) {
        List<Group> groups = new ArrayList<Group>();
        int i = 0;
        int n = fragments.size();
        while (i < n) {
            Fragment f = fragments.get(i);
            if (!f.isMergeable()) {
                groups.add(new Group(f.raw, i, 1, null)); // 不透明：原样执行
                i++;
                continue;
            }
            // 收集同前缀、未超限的连续片段
            int j = i;
            int rows = 0;
            int bytes = f.prefix.length() + f.tuples.length();
            StringBuilder sb = new StringBuilder(bytes + 64).append(f.prefix).append(f.tuples);
            List<Integer> counts = new ArrayList<Integer>();
            counts.add(f.tupleCount);
            rows += f.tupleCount;
            j++;
            while (j < n) {
                Fragment g = fragments.get(j);
                if (!g.isMergeable() || !f.prefix.equals(g.prefix)) {
                    break;
                }
                int addBytes = 1 + g.tuples.length();
                if (rows + g.tupleCount > maxRows || bytes + addBytes > maxBytes) {
                    break;
                }
                sb.append(',').append(g.tuples);
                counts.add(g.tupleCount);
                rows += g.tupleCount;
                bytes += addBytes;
                j++;
            }
            int[] cs = new int[counts.size()];
            for (int k = 0; k < cs.length; k++) {
                cs[k] = counts.get(k);
            }
            groups.add(new Group(sb.toString(), i, j - i, cs));
            i = j;
        }
        return groups;
    }

    // ------------------------------------------------------------------
    // 预处理路径（PreparedStatement.addBatch）
    // ------------------------------------------------------------------

    /** 预处理模板的拆分结果：前缀 + 元组模板。 */
    public static final class Template {
        /** 字面量之前的部分（含 VALUES），如 {@code INSERT INTO t VALUES}。 */
        public final String prefix;
        /** 元组模板，如 {@code " (?, ?)"}。 */
        public final String tupleTemplate;

        Template(String prefix, String tupleTemplate) {
            this.prefix = prefix;
            this.tupleTemplate = tupleTemplate;
        }
    }

    /**
     * 拆分预处理模板。仅当 VALUES 之前没有占位符、且元组部分形态可靠时返回
     * 非 null（否则调用方回退逐条渲染执行）。
     *
     * @param sql          预处理模板（含 {@code ?}）
     * @param paramCount   模板占位符总数
     */
    public static Template splitTemplate(String sql, int paramCount) {
        if (sql == null) {
            return null;
        }
        int idx = findValuesClause(sql);
        if (idx < 0) {
            return null;
        }
        String prefix = sql.substring(0, idx + VALUES_LEN);
        String tupleTemplate = sql.substring(idx + VALUES_LEN);
        if (countTopLevelTuples(tupleTemplate) < 0) {
            return null; // 元组形态不可靠（含尾随子句等）
        }
        int prefixParams;
        int tupleParams;
        try {
            prefixParams = ParamRenderer.countPlaceholders(prefix);
            tupleParams = ParamRenderer.countPlaceholders(tupleTemplate);
        } catch (java.sql.SQLException e) {
            return null; // 词法不干净 → 不合并
        }
        if (prefixParams != 0 || tupleParams != paramCount) {
            return null; // 占位符跨 VALUES 分布 → 无法按元组渲染
        }
        return new Template(prefix.trim(), tupleTemplate);
    }

    /** 渲染后的元组文本 → 片段（渲染由 ParamRenderer 完成，见调用方）。 */
    public static Fragment fragmentOfRenderedTuple(String prefix, String renderedTuple) {
        int count = countTopLevelTuples(renderedTuple);
        if (count < 0) {
            return Fragment.opaque(null);
        }
        return Fragment.mergeable(prefix, normalize(renderedTuple), count);
    }

    // ------------------------------------------------------------------
    // 词法
    // ------------------------------------------------------------------

    private static final String VALUES_KW = "VALUES";
    private static final int VALUES_LEN = VALUES_KW.length();

    /**
     * 查找 VALUES 子句：关键字之后（跳过空白/注释）必须是 {@code (}——
     * 避免把名为 {@code values} 的表名当成子句（{@code INSERT INTO values VALUES (1)}）。
     *
     * @return VALUES 关键字起始下标；未找到返回 -1
     */
    static int findValuesClause(String sql) {
        int from = 0;
        while (true) {
            int i = findKeyword(sql, VALUES_KW, from);
            if (i < 0) {
                return -1;
            }
            int after = skipWsAndComments(sql, i + VALUES_LEN);
            if (after < sql.length() && sql.charAt(after) == '(') {
                return i;
            }
            from = i + VALUES_LEN; // 是表名/列名等 → 继续找下一个
        }
    }

    /** 在字符串/注释/引用标识符之外查找关键字（词边界、大小写不敏感）。 */
    public static int findKeyword(String sql, String keyword, int from) {
        int n = sql.length();
        int i = from;
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {
                i = skipSingleQuoted(sql, i);
            } else if (c == '"' || c == '`') {
                i = skipQuotedIdentifier(sql, i, c);
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                i = skipLineComment(sql, i);
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                i = skipBlockComment(sql, i);
            } else if (matchesKeywordAt(sql, i, keyword)) {
                return i;
            } else {
                i++;
            }
        }
        return -1;
    }

    private static boolean matchesKeywordAt(String sql, int i, String keyword) {
        if (!sql.regionMatches(true, i, keyword, 0, keyword.length())) {
            return false;
        }
        boolean startOk = i == 0 || !isWordChar(sql.charAt(i - 1));
        int end = i + keyword.length();
        boolean endOk = end >= sql.length() || !isWordChar(sql.charAt(end));
        return startOk && endOk;
    }

    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    /**
     * 统计顶层元组个数：文本须严格形如 {@code (…)[, (…)]*}（允许尾随空白、
     * 注释与分号）。形态不符返回 -1——调用方据此回退不合并。
     */
    public static int countTopLevelTuples(String text) {
        if (text == null) {
            return -1;
        }
        String s = text;
        int i = skipWsAndComments(s, 0);
        int tuples = 0;
        while (true) {
            if (i >= s.length() || s.charAt(i) != '(') {
                break;
            }
            int after = skipBalancedParens(s, i);
            if (after < 0) {
                return -1; // 括号不配平
            }
            tuples++;
            i = skipWsAndComments(s, after);
            if (i >= s.length()) {
                break;
            }
            if (s.charAt(i) == ';') {
                i = skipWsAndComments(s, i + 1);
                break; // 分号后只允许空白/注释
            }
            if (s.charAt(i) != ',') {
                return -1; // 出现了元组之外的子句（ON CONFLICT / RETURNING 等）
            }
            i = skipWsAndComments(s, i + 1);
        }
        return (tuples > 0 && i >= s.length()) ? tuples : -1;
    }

    /** {@code s[i]=='('} 起跳到配对 {@code ')'} 之后；不配平返回 -1。 */
    private static int skipBalancedParens(String s, int i) {
        int n = s.length();
        int depth = 0;
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\'') {
                i = skipSingleQuoted(s, i);
            } else if (c == '"' || c == '`') {
                i = skipQuotedIdentifier(s, i, c);
            } else if (c == '-' && i + 1 < n && s.charAt(i + 1) == '-') {
                i = skipLineComment(s, i);
            } else if (c == '/' && i + 1 < n && s.charAt(i + 1) == '*') {
                i = skipBlockComment(s, i);
            } else if (c == '(') {
                depth++;
                i++;
            } else if (c == ')') {
                depth--;
                i++;
                if (depth == 0) {
                    return i;
                }
                if (depth < 0) {
                    return -1;
                }
            } else {
                i++;
            }
        }
        return -1;
    }

    /** 跳过空白与注释（行/块）。 */
    static int skipWsAndComments(String s, int i) {
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '-' && i + 1 < n && s.charAt(i + 1) == '-') {
                i = skipLineComment(s, i);
            } else if (c == '/' && i + 1 < n && s.charAt(i + 1) == '*') {
                i = skipBlockComment(s, i);
            } else {
                break;
            }
        }
        return i;
    }

    /** 单引号字符串（{@code ''} 为转义）；未闭合返回串尾。 */
    private static int skipSingleQuoted(String s, int i) {
        int n = s.length();
        int j = i + 1;
        while (j < n) {
            if (s.charAt(j) == '\'') {
                if (j + 1 < n && s.charAt(j + 1) == '\'') {
                    j += 2;
                    continue;
                }
                return j + 1;
            }
            j++;
        }
        return n;
    }

    /** 引用标识符（{@code ""} / {@code ``} 为转义）；未闭合返回串尾。 */
    private static int skipQuotedIdentifier(String s, int i, char quote) {
        int n = s.length();
        int j = i + 1;
        while (j < n) {
            if (s.charAt(j) == quote) {
                if (j + 1 < n && s.charAt(j + 1) == quote) {
                    j += 2;
                    continue;
                }
                return j + 1;
            }
            j++;
        }
        return n;
    }

    private static int skipLineComment(String s, int i) {
        int nl = s.indexOf('\n', i + 2);
        return nl < 0 ? s.length() : nl;
    }

    private static int skipBlockComment(String s, int i) {
        int end = s.indexOf("*/", i + 2);
        return end < 0 ? s.length() : end + 2;
    }

    /** 元组文本归一：去尾随空白/注释/分号（合并时统一由调用方拼接分隔）。 */
    static String normalize(String tuples) {
        int end = tuples.length();
        // 尾部裁剪：空白 / 分号 / 注释
        while (end > 0) {
            char c = tuples.charAt(end - 1);
            if (Character.isWhitespace(c) || c == ';') {
                end--;
                continue;
            }
            // 尾随块注释 /**/
            int blockStart = tuples.lastIndexOf("*/", end);
            if (blockStart >= 0 && blockStart + 2 == end) {
                int open = tuples.lastIndexOf("/*", blockStart);
                if (open >= 0) {
                    end = open;
                    continue;
                }
            }
            break;
        }
        return tuples.substring(0, end);
    }
}
