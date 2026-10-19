package cn.qinwh.qsdb.jdbc.transport;

/**
 * 查询选项（设计文档 §三.4）。
 *
 * <p>不可变。各字段的线上落点（2026-10-19 协议对账后）：
 * <ul>
 *   <li>{@code timeoutMs} → 请求头 {@code X-QSDB-Timeout-Ms}（毫秒；
 *       <b>服务端侧</b>超时——超时由内核终止查询并回 {@code QUERY_TIMEOUT}
 *       错误体；0 = 不限，不发送头）；</li>
 *   <li>{@code maxRows} → <b>纯驱动侧截断</b>（JDBC 语义："最多返回 n 行"）。
 *       **不上线**——内核 body {@code maxRows} 是结果护栏的<b>超限拒绝</b>
 *       阈值（R1：绝不返回部分数据），与 JDBC 截断语义相反，透传会把
 *       {@code setMaxRows(n)} 且结果超 n 的正常用法变成硬错误；</li>
 *   <li>{@code liftResultGuard} → 仅显式 {@code setMaxRows(0)}（JDBC"不限"）
 *       时随请求发 body {@code maxRows:0}，<b>解除</b>本请求的服务端护栏
 *       ——否则应用声明"不限"却在服务端护栏处撞 {@code RESULT_TOO_LARGE}。
 *       护栏是服务端运维配置，默认（未显式声明不限）继续生效；</li>
 *   <li>{@code readFormat} → body {@code "format"}（纯传输层策略，不上线）。</li>
 * </ul>
 */
public final class QueryOptions {

    /** 查询超时（毫秒）；0 = 不限。 */
    public final long timeoutMs;

    /** 行数上限；0 = 不限。纯驱动侧截断（JDBC 语义），不随请求上线。 */
    public final long maxRows;

    /**
     * true = 显式声明"不限"（{@code Statement.setMaxRows(0)}）→ 随请求发
     * body {@code maxRows:0} 解除本请求的服务端结果护栏。
     */
    public final boolean liftResultGuard;

    /**
     * 服务端参数化通道：已序列化的 body {@code "params"} JSON 数组文本
     * （含方括号；null = 无参数，不发字段）。占位符仍留在 SQL 文本里
     * （{@code ?}，内核解析前固化 {@code $n}），值经本数组进内核绑定器——
     * **值从不进入 SQL 文本**（2026-10-19 接入，P1）。
     */
    public final String paramsJson;

    /**
     * 目标库（内核十六批 M-B：body {@code "database"}——优先级最高：显式
     * database &gt; 会话默认库 &gt; {@code "default"}；库未挂载 404）。
     * null = 不发字段。多库面（P2）由 {@code setCatalog}/连接属性
     * {@code database} 决定。
     */
    public final String database;

    /**
     * 流式读取块大小提示（P3：&gt;0 = 走 rows 流式通道
     * {@code Connection: close}，逐块消费内存恒定；0 = 一次性响应）。
     * 纯传输层策略（不随请求上线——服务端按连接语义自行分批）。
     */
    public final int fetchSize;

    /**
     * 读取格式策略：{@code auto}（默认，优先二进制行式通道 rows，旧 server
     * 自动退回 json）| {@code json}（强制 JSON——稳定路径）| {@code rows}
     * （强制 rows，失败不降级）。null 等价于 auto。
     */
    public final String readFormat;

    public static final QueryOptions DEFAULT = new QueryOptions(0, 0, null);

    public QueryOptions(long timeoutMs, long maxRows) {
        this(timeoutMs, maxRows, null);
    }

    public QueryOptions(long timeoutMs, long maxRows, String readFormat) {
        this(timeoutMs, maxRows, readFormat, false, null, null, 0);
    }

    public QueryOptions(long timeoutMs, long maxRows, String readFormat, boolean liftResultGuard) {
        this(timeoutMs, maxRows, readFormat, liftResultGuard, null, null, 0);
    }

    public QueryOptions(long timeoutMs, long maxRows, String readFormat, boolean liftResultGuard,
                        String paramsJson) {
        this(timeoutMs, maxRows, readFormat, liftResultGuard, paramsJson, null, 0);
    }

    public QueryOptions(long timeoutMs, long maxRows, String readFormat, boolean liftResultGuard,
                        String paramsJson, String database) {
        this(timeoutMs, maxRows, readFormat, liftResultGuard, paramsJson, database, 0);
    }

    /**
     * 全参形态。
     *
     * @param liftResultGuard true = 本请求解除服务端结果护栏（发
     *                        {@code maxRows:0}）；见类注释——仅显式"不限"时为 true
     * @param paramsJson      已序列化的 params JSON 数组文本（含方括号）；
     *                        null = 无参数
     * @param database        目标库；null = 不发字段
     * @param fetchSize       流式块提示；&gt;0 = 流式通道（见字段注释）
     */
    public QueryOptions(long timeoutMs, long maxRows, String readFormat, boolean liftResultGuard,
                        String paramsJson, String database, int fetchSize) {
        this.timeoutMs = timeoutMs;
        this.maxRows = maxRows;
        this.readFormat = readFormat;
        this.liftResultGuard = liftResultGuard;
        this.paramsJson = paramsJson;
        this.database = database;
        this.fetchSize = fetchSize;
    }
}
