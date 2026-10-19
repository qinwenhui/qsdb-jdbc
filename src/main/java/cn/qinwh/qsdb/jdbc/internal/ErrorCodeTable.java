package cn.qinwh.qsdb.jdbc.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * QSDB 错误码本地表（内核 {@code docs/error-codes.md} §三 的驱动侧副本，
 * 2026-10-19 对齐，28 码——补齐内核十六批～二十批新增的 5 码：
 * {@code SESSION_REVOKED} / {@code RESULT_TOO_LARGE} / {@code CURSOR_TOO_OLD}
 * / {@code SEGMENT_REFERENCED} / {@code TOO_MANY_SESSIONS}）。
 *
 * <p><b>角色</b>：仅作运行期码表（{@code GET /api/v1/error-codes}，免鉴权）
 * 不可用时的<b>兜底</b>——主路径是动态拉取（内核加码驱动自动跟上），
 * 本地表保证冷启动/断网/旧 server 场景可用。
 *
 * <p>SQLState 归属权在驱动（内核仅建议）：绝大多数码直接采用内核建议
 * （Postgres 风格 5 字符码，语义精确、JDBC 生态通用）；一个驱动契约特例：
 * {@code QUERY_TIMEOUT → HYT00}（设计文档 §十 契约，JDBC 生态惯用；
 * 内核建议 57014）。{@code QUERY_CANCELLED} 与内核建议一致（HY008，
 * 2026-10-13 内核采纳驱动反馈后对齐）。
 *
 * <p>行为分流的两个关键族：
 * <ul>
 *   <li><b>401 双码</b>：{@code UNAUTHORIZED}（凭证错，连接不可自愈）与
 *       {@code SESSION_REVOKED}（会话被踢/服务端重启/空闲回收——持 login
 *       凭证时重登即恢复）。SQLState 同为 28000，动作完全不同，消费方按
 *       code 分流（驱动在会话面按此分档重登）。</li>
 *   <li><b>503 三码</b>：{@code TOO_MANY_QUERIES} / {@code TOO_MANY_SESSIONS}
 *       （均为容量瞬时满）retry=yes 可退避重试；{@code READ_ONLY} retry=no
 *       须换实例。另注意与 400 {@code QUERY_MEMORY_LIMIT}（自己超配额，改
 *       查询）动作相反。</li>
 * </ul>
 *
 * <p>internal API——不在驱动公开面上。
 */
public final class ErrorCodeTable {

    private ErrorCodeTable() {
    }

    /** 单码条目：SQLState + 是否可重试（retry=yes 表示瞬时故障，退避重试有意义）。 */
    public static final class Entry {
        public final String sqlState;
        public final boolean retryable;

        public Entry(String sqlState, boolean retryable) {
            this.sqlState = sqlState;
            this.retryable = retryable;
        }
    }

    private static final Map<String, Entry> LOCAL = new HashMap<String, Entry>();

    private static void put(String code, String sqlState, boolean retryable) {
        LOCAL.put(code, new Entry(sqlState, retryable));
    }

    static {
        // ---- query 族 ----
        put("QUERY_TIMEOUT", "HYT00", false); // 驱动契约 §十（内核建议 57014）
        put("QUERY_CANCELLED", "HY008", false); // JDBC 标准 operation canceled
        put("QUERY_MEMORY_LIMIT", "53200", false);
        // 结果护栏（内核十七批 R1）：结果体量/行数超限 → 恒定拒绝，绝不返回
        // 部分数据。动作 = 加 LIMIT / 调大 Statement.setMaxRows（驱动透传
        // maxRows 护栏覆盖）/ 换流式读——不是可重试故障
        put("RESULT_TOO_LARGE", "54000", false);
        put("QUERY_PANIC", "XX000", false);
        put("TABLE_NOT_FOUND", "42P01", false);
        put("COLUMN_NOT_FOUND", "42703", false);
        put("PARSE_ERROR", "42601", false);
        put("TYPE_ERROR", "42804", false);
        put("UNSUPPORTED", "0A000", false);
        // ---- resource / request 族 ----
        put("ALREADY_EXISTS", "42P07", false);
        put("INVALID_ARGUMENT", "42000", false);
        put("INVALID_IPC", "08P01", false);
        put("NOT_FOUND", "42704", false);
        // 409 并发族：两个码**同为 40001 但动作相反**（F24/F26）——
        // WRITE_CONFLICT：CAS 未命中 expected_generation → 重读最新 generation
        //                 再决定（不是盲目重试）；
        // STALE_WRITER ：本进程 --writer-epoch 落后于盘上写者身份代 → 必须
        //                 运维抬高 epoch 重启（重试无用）。
        // 二者 retry 均为 no（盲目重试都无意义），故区分只能靠 **code 本身**——
        // 异常 message 恒带 `[code=…]`，工具按 code 分流
        put("WRITE_CONFLICT", "40001", false);
        put("STALE_WRITER", "40001", false);
        // watch 游标（内核订阅面）：缓冲滚过/服务端重启——原游标永远不会好，
        // 动作 = 一次全量对账后从当前续订（重试同一游标无意义）
        put("CURSOR_TOO_OLD", "HY000", false);
        // 文件回收面（内核二十批）：段被存活 manifest 引用，拒绝进回收站
        put("SEGMENT_REFERENCED", "55006", false);
        put("NOT_FOUND", "42704", false);
        // ---- auth 族 ----
        put("UNAUTHORIZED", "28000", false);
        // 会话被踢/服务端重启/空闲回收：**与 UNAUTHORIZED 同 SQLState 但动作
        // 不同**——持 login 凭证时重登即恢复（读路径可安全重发一次：认证闸在
        // 执行前，401 = 语句从未运行）；无凭证（静态 token）时才不可自愈
        put("SESSION_REVOKED", "28000", false);
        put("FORBIDDEN", "42501", false);
        // ---- capacity 族（同 503 三个码，按 code 分流勿按状态码）----
        put("TOO_MANY_QUERIES", "53300", true);  // 并发条数满：退避重试
        put("TOO_MANY_SESSIONS", "08004", true); // 会话数上限：退避重试（等会话退出）
        put("MEMORY_PRESSURE", "53000", true);   // 全库内存预算被他人占用：退避重试
        put("READ_ONLY", "25006", false);        // 只读库：永不重试，换实例
        // ---- storage / internal 族 ----
        put("METADATA_ERROR", "XX000", false);
        put("IO_ERROR", "58030", true);
        put("INTERNAL", "XX000", false);
    }

    /** 本地兜底表（只读视图）。 */
    public static Map<String, Entry> local() {
        return Collections.unmodifiableMap(LOCAL);
    }

    /** 已知码查询（本地兜底表）。 */
    public static Entry lookup(String code) {
        return code == null ? null : LOCAL.get(code);
    }
}
