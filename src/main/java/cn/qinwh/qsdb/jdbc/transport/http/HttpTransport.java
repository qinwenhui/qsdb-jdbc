package cn.qinwh.qsdb.jdbc.transport.http;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import cn.qinwh.qsdb.jdbc.QsdbException;
import cn.qinwh.qsdb.jdbc.internal.Errors;
import cn.qinwh.qsdb.jdbc.internal.ErrorCodeTable;
import cn.qinwh.qsdb.jdbc.internal.Http;
import cn.qinwh.qsdb.jdbc.internal.MiniJson;
import cn.qinwh.qsdb.jdbc.internal.RowsDecoder;
import cn.qinwh.qsdb.jdbc.internal.RowsEncoder;
import cn.qinwh.qsdb.jdbc.internal.RowsStreamReader;
import cn.qinwh.qsdb.jdbc.transport.ColMeta;
import cn.qinwh.qsdb.jdbc.transport.DbInfo;
import cn.qinwh.qsdb.jdbc.transport.PartitionInfo;
import cn.qinwh.qsdb.jdbc.transport.QueryOptions;
import cn.qinwh.qsdb.jdbc.transport.QueryResponse;
import cn.qinwh.qsdb.jdbc.transport.RowsStream;
import cn.qinwh.qsdb.jdbc.transport.TableSchema;
import cn.qinwh.qsdb.jdbc.transport.Transport;
import cn.qinwh.qsdb.jdbc.transport.Value;

/**
 * v1 传输层实现：QSDB HTTP API（设计文档 §五契约；错误码体系对齐内核
 * {@code docs/error-codes.md}，2026-10-19 对账至内核二十批）。
 *
 * <p>端点使用：
 * <ul>
 *   <li>{@code POST /api/v1/query}——body {@code {"sql":…,"format":"json"}}
 *       （仅显式"不限"时附 {@code "maxRows":0} 解除服务端护栏）；超时经
 *       {@code X-QSDB-Timeout-Ms} 头下发（毫秒，**服务端侧**终止——2026-10-19
 *       修复此前"文档宣称、实现缺失"）；查询 id 经 {@code X-QSDB-Query-Id}
 *       头由客户端指定（J11：执行前注册 → {@code cancel()} 零扫描零误伤
 *       精确取消）；</li>
 *   <li>{@code GET /api/v1/tables}（含 {@code ?withSchema=1} 批量形态）、
 *       {@code GET /api/v1/tables/{t}/schema}；</li>
 *   <li>{@code GET /api/v1/error-codes}——码表运行期拉取（免鉴权静态），
 *       {@code code → SQLState/retry} 映射自动跟随内核加码；</li>
 *   <li>{@code GET /health}——存活探测。</li>
 * </ul>
 *
 * <p>错误映射：错误体 {@code {"error","code"}}（code 契约级稳定）。code 的
 * SQLState 优先取运行期码表（内核建议），本地兜底表
 * {@link ErrorCodeTable} 服务冷启动/旧 server；QUERY_TIMEOUT 恒 HYT00
 * （驱动 §十 契约）。503 三码（TOO_MANY_QUERIES / TOO_MANY_SESSIONS 可退避
 * / READ_ONLY 永不重试）按 code 分流，不按状态码。
 *
 * <p>客户端读超时 = 查询超时 + 30s 余量：内核按同一超时主动终止查询，
 * 余量保证正常路径下由 server 先返回、客户端不抢先断链。
 */
public final class HttpTransport implements Transport {

    /** 查询超时之外给 server 响应返回的余量（毫秒）。 */
    private static final long READ_TIMEOUT_SLACK_MS = 30_000L;

    private final String baseUrl;
    private final String staticToken;   // 静态 Bearer token（server --token；null = 匿名）
    private final String loginUser;     // login 凭证（null = 不用会话面）
    private final String loginPassword;
    private final long connectTimeoutMs;
    private final long defaultQueryTimeoutMs;

    /** 正在执行的查询 id（客户端指定，J11）——cancel 的精确目标。 */
    private final AtomicReference<String> inFlightQueryId = new AtomicReference<String>();

    /** 运行期码表（code → [sqlState, retryable]）；null = 尚未尝试拉取。 */
    private volatile Map<String, ErrorCodeTable.Entry> dynamicCodes;

    /** auto 模式下探测到 server 不支持 rows 读（旧 server）→ 本连接后续走 json。 */
    private volatile boolean rowsReadUnsupported;

    // ---- 会话态（内核十八批 C1：login 签发的进程态凭证）----
    private volatile String sessionToken;
    private volatile String sessionId;
    private volatile String sessionRole;
    private volatile String sessionDefaultDb;

    /**
     * @param host                 server 主机
     * @param port                 server 端口
     * @param staticToken          静态 Bearer token（{@code --token}；null = 匿名）
     * @param loginUser            login 用户（非 null 时 {@code user+password} →
     *                             Basic 换进程态会话 token，懒登录 + SESSION_REVOKED
     *                             自动重登分档）
     * @param loginPassword        login 口令
     * @param connectTimeoutMs     TCP 连接建立超时
     * @param defaultQueryTimeoutMs 查询默认超时（0 = 不限）
     * @throws QsdbException ssl=true 等 v1 不支持的配置
     */
    public HttpTransport(String host, int port, String staticToken,
                         String loginUser, String loginPassword,
                         long connectTimeoutMs, long defaultQueryTimeoutMs) throws QsdbException {
        this.baseUrl = "http://" + host + ":" + port;
        this.staticToken = staticToken;
        this.loginUser = loginUser;
        this.loginPassword = loginPassword;
        this.connectTimeoutMs = connectTimeoutMs;
        this.defaultQueryTimeoutMs = defaultQueryTimeoutMs;
    }

    /**
     * 执行查询。格式选择（{@link QueryOptions#readFormat}）：
     * <ul>
     *   <li>{@code json}——强制 JSON（稳定路径，行为与引入 rows 前一致）；</li>
     *   <li>{@code auto}（默认）——优先 {@code rows} 二进制行式通道；若 server
     *       明确不支持该格式（旧 server），或 payload 含驱动无法表示的类型，
     *       则退回 JSON 并（对 server 不支持的情况）记住，不再重试；</li>
     *   <li>{@code rows}——强制 rows，任何失败原样抛出（不静默降级）。</li>
     * </ul>
     * 注意：payload **畸形**（偏移量报错）属于真实故障，auto 模式也**不**
     * 降级——降级只针对"格式不可用/类型不受支持"两类确定性情形，避免把
     * 解码缺陷藏起来。
     */
    @Override
    public QueryResponse query(String sql, QueryOptions opts) throws QsdbException {
        String mode = opts != null && opts.readFormat != null ? opts.readFormat : "auto";
        boolean tryRows = !"json".equals(mode) && !("auto".equals(mode) && rowsReadUnsupported);
        if (tryRows) {
            try {
                return queryWithFormat(sql, opts, "rows");
            } catch (QsdbException e) {
                if ("rows".equals(mode)) {
                    throw e; // 强制 rows：不降级
                }
                if (isFormatUnsupported(e)) {
                    rowsReadUnsupported = true; // 旧 server：本连接后续直接走 json
                } else if (!isTypeUnsupported(e) && !isEmptyResultUnsupported(e)) {
                    throw e; // 畸形 payload 等真实故障不掩盖
                }
            }
        }
        return queryWithFormat(sql, opts, "json");
    }

    /** server 明确应答"不认识这个 format"（旧 server）。 */
    private static boolean isFormatUnsupported(QsdbException e) {
        String m = e.getMessage();
        return m != null && m.contains("format")
                && ("0A000".equals(e.getSQLState()) || "42000".equals(e.getSQLState()));
    }

    /** rows payload 合法但含驱动无法表示的类型（如 INTERVAL）——json 仍可答。 */
    private static boolean isTypeUnsupported(QsdbException e) {
        String m = e.getMessage();
        return m != null && m.contains("unsupported column type tag");
    }

    /**
     * 零行结果 rows 通道当前拒绝（内核 <b>J15</b>：{@code rows: empty batches}）。
     * 空结果完全正常（{@code LIMIT 0} 探测、{@code WHERE} 无命中），故本次查询
     * 退回 json——但**不记住**：后续有行的查询仍走 rows。
     */
    private static boolean isEmptyResultUnsupported(QsdbException e) {
        String m = e.getMessage();
        return m != null && m.contains("empty batches");
    }

    private QueryResponse queryWithFormat(String sql, QueryOptions opts, String format)
            throws QsdbException {
        long timeoutMs = opts != null && opts.timeoutMs > 0 ? opts.timeoutMs : defaultQueryTimeoutMs;
        StringBuilder body = new StringBuilder("{\"sql\":");
        MiniJson.writeString(body, sql == null ? "" : sql);
        body.append(",\"format\":\"").append(format).append("\"");
        // 显式"不限"（setMaxRows(0)）→ 发 body maxRows:0 解除本请求的服务端
        // 结果护栏（JDBC "不限" 语义对齐）。其余情况**不发**：内核 maxRows 是
        // 超限拒绝阈值（R1 绝不部分返回），与 JDBC 截断语义相反——setMaxRows(n)
        // 的截断在驱动侧做，透传 n 反而把正常用法变成硬错误
        if (opts != null && opts.liftResultGuard) {
            body.append(",\"maxRows\":0");
        }
        // 服务端参数化通道（P1）：值不进 SQL 文本——绑定在内核绑定器里完成。
        // paramsJson 为预序列化 JSON 数组（含方括号）；null/空 = 不发字段
        if (opts != null && opts.paramsJson != null) {
            body.append(",\"params\":").append(opts.paramsJson);
        }
        // 多库（P2）：body database 优先级最高（显式 > 会话默认库 > "default"）
        if (opts != null && opts.database != null && !opts.database.isEmpty()) {
            body.append(",\"database\":");
            MiniJson.writeString(body, opts.database);
        }
        body.append("}");
        // J11：客户端指定 query id（合法字符 [A-Za-z0-9._:-]，UUID 满足）——
        // server 采纳后 cancel 可直达 /queries/{id}/cancel，零扫描零误伤
        String clientQueryId = "jdbc-" + UUID.randomUUID();
        inFlightQueryId.set(clientQueryId);
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("X-QSDB-Query-Id", clientQueryId);
        if (timeoutMs > 0) {
            // 服务端侧超时（2026-10-19 修复此前注释宣称但从未发送的缺口）：
            // 超时由内核终止查询并回 QUERY_TIMEOUT 错误体，而非客户端干等
            headers.put("X-QSDB-Timeout-Ms", String.valueOf(timeoutMs));
        }
        Http.Response resp;
        try {
            resp = sessionPostJson("/api/v1/query", headers, body.toString(),
                    connectTimeoutMs, readTimeoutFor(timeoutMs));
        } finally {
            inFlightQueryId.compareAndSet(clientQueryId, null);
        }
        if (!resp.is2xx()) {
            throw errorForStatus(resp, "query");
        }
        // 服务端执行耗时（十九批Ⅷ统一响应头；不含序列化/网络/客户端解码）。
        // 旧 server 无头 → 回退 body 的 elapsedMs 字段（JSON 通道）→ 均无则 null
        Double elapsed = elapsedHeaderOf(resp);
        if ("rows".equals(format) && RowsDecoder.looksLikeRows(resp.bytes)) {
            // 二进制行式通道：不解 JSON，直接解 payload（列描述随头下发）。
            // 注意 magic 嗅探不可省——非 SELECT 语句（DDL/DML）即使点名
            // format=rows，内核仍回 JSON 回执（没有行可编码），此时落到下方
            // 的 JSON 分支处理
            RowsDecoder.Decoded d = RowsDecoder.decode(resp.bytes);
            return QueryResponse.ofQuery(d.columns, d.rows, elapsed, clientQueryId);
        }
        return parseJsonQueryPayload(resp.bytes, elapsed);
    }

    /** JSON 载荷解析（查询结果 schema 形态 / J12 结构化回执形态）。 */
    private QueryResponse parseJsonQueryPayload(byte[] payload, Double headerElapsed)
            throws QsdbException {
        Map<String, Object> root = MiniJson.parseObject(new String(payload, StandardCharsets.UTF_8));
        Double elapsed = headerElapsed;
        Object schemaObj = root.get("schema");
        String queryId = str(root.get("queryId")); // 响应回显（采用客户端 id 或冲突回退后的实际 id）
        if (schemaObj instanceof List) {
            if (elapsed == null) {
                elapsed = elapsedOf(root.get("elapsedMs")); // 旧 server 回退 body 字段
            }
            return parseQueryResponse((List<?>) schemaObj, root.get("rows"), elapsed, queryId);
        }
        Object message = root.get("message");
        if (message instanceof String) {
            // J12：结构化回执（DML 带 command/rowsAffected；DDL 两者皆无——
            // null 不等于 0，故不做默认填充）
            Object affected = root.get("rowsAffected");
            if (elapsed == null) {
                elapsed = elapsedOf(root.get("elapsedMs"));
            }
            return QueryResponse.ofMessage((String) message, elapsed,
                    queryId, str(root.get("command")),
                    affected instanceof Number ? Long.valueOf(((Number) affected).longValue()) : null);
        }
        throw Errors.connection("unexpected response body from /api/v1/query (neither schema nor message)", null);
    }

    // ------------------------------------------------------------------
    // 流式查询（P3：setFetchSize 真流式——rows 通道 + Connection: close）
    // ------------------------------------------------------------------

    @Override
    public Transport.StreamedResult queryStream(String sql, QueryOptions opts)
            throws QsdbException {
        // 格式强制 rows（流式契约）；请求体与一次性通道同构（复用其拼装规则）
        StringBuilder body = new StringBuilder("{\"sql\":");
        MiniJson.writeString(body, sql == null ? "" : sql);
        body.append(",\"format\":\"rows\"");
        if (opts != null && opts.liftResultGuard) {
            body.append(",\"maxRows\":0");
        }
        if (opts != null && opts.paramsJson != null) {
            body.append(",\"params\":").append(opts.paramsJson);
        }
        if (opts != null && opts.database != null && !opts.database.isEmpty()) {
            body.append(",\"database\":");
            MiniJson.writeString(body, opts.database);
        }
        body.append("}");
        long timeoutMs = opts != null && opts.timeoutMs > 0 ? opts.timeoutMs : defaultQueryTimeoutMs;
        String clientQueryId = "jdbc-" + UUID.randomUUID();
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("X-QSDB-Query-Id", clientQueryId);
        if (timeoutMs > 0) {
            headers.put("X-QSDB-Timeout-Ms", String.valueOf(timeoutMs));
        }
        ensureSession();
        Http.StreamResponse resp = openStream(headers, body.toString(),
                readTimeoutFor(timeoutMs));
        if (!resp.is2xx()) {
            QsdbException err = errorForStatus(
                    Http.Response.of(resp.status, resp.errorBytes()), "query stream");
            resp.close(); // Connection: close——错误路径同样断链清理
            // SESSION_REVOKED：读语义（认证闸在执行前）→ 重登 + 重发一次
            if ("SESSION_REVOKED".equals(err.getServerCode()) && loginUser != null) {
                sessionToken = null;
                login();
                return queryStream(sql, opts);
            }
            throw err;
        }
        try {
            // magic 嗅探（8 字节）：非 SELECT 语句内核回 JSON 回执——读尽剩余，
            // 一次性解析交付；SELECT → 流式交付
            byte[] head = readUpTo(resp.body(), RowsEncoder.MAGIC.length);
            if (!RowsDecoder.looksLikeRows(head)) {
                byte[] rest = readRemaining(resp.body());
                byte[] all = new byte[head.length + rest.length];
                System.arraycopy(head, 0, all, 0, head.length);
                System.arraycopy(rest, 0, all, head.length, rest.length);
                Double elapsed = elapsedHeaderOf(resp);
                QueryResponse oneShot = parseJsonQueryPayload(all, elapsed);
                return new Transport.StreamedResult(null, oneShot);
            }
            // head 恰为 magic（已消费）——流从 version 字段开始
            RowsStreamReader reader = new RowsStreamReader(resp.body());
            return new Transport.StreamedResult(
                    new RowsStreamImpl(resp, reader, clientQueryId), null);
        } catch (QsdbException e) {
            resp.close(); // 半途解析失败不把半死的连接留在无主状态
            throw e;
        } catch (RuntimeException e) {
            resp.close();
            throw e;
        }
    }

    /** 会话感知的流式打开（IOException → 08S01；SESSION_REVOKED 由调用方处理）。 */
    private Http.StreamResponse openStream(Map<String, String> headers, String body,
                                           long readTimeoutMs) throws QsdbException {
        try {
            return Http.postStream(baseUrl + "/api/v1/query", effectiveToken(), headers,
                    body, connectTimeoutMs, readTimeoutMs);
        } catch (IOException e) {
            throw Errors.connection("query stream request failed: " + e.getMessage(), e);
        }
    }

    /** 阻塞读满 n 字节（不足即流结束——魔法嗅探的"没读满"由调用方判别）。 */
    private static byte[] readUpTo(java.io.InputStream in, int n) throws QsdbException {
        byte[] out = new byte[n];
        int read = 0;
        try {
            while (read < n) {
                int k = in.read(out, read, n - read);
                if (k < 0) {
                    break;
                }
                read += k;
            }
        } catch (IOException e) {
            throw Errors.connection("query stream read failed: " + e.getMessage(), e);
        }
        if (read == n) {
            return out;
        }
        byte[] trimmed = new byte[read];
        System.arraycopy(out, 0, trimmed, 0, read);
        return trimmed;
    }

    /** 读尽剩余字节（JSON 回执路径——回执体小，全量读入无压力）。 */
    private static byte[] readRemaining(java.io.InputStream in) throws QsdbException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        try {
            int n;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
            }
        } catch (IOException e) {
            throw Errors.connection("query stream read failed: " + e.getMessage(), e);
        }
        return out.toByteArray();
    }

    /** {@link RowsStream} 的 HTTP 实现：close = 断链（传输段中止语义）。 */
    private static final class RowsStreamImpl implements RowsStream {
        private final Http.StreamResponse resp;
        private final RowsStreamReader reader;
        private final String queryId;
        private boolean closed;

        RowsStreamImpl(Http.StreamResponse resp, RowsStreamReader reader, String queryId) {
            this.resp = resp;
            this.reader = reader;
            this.queryId = queryId;
        }

        /** 本流的 server 查询标识（响应回显；诊断/审计用）。 */
        String queryId() {
            return queryId;
        }

        @Override
        public List<ColMeta> columns() {
            return reader.columns();
        }

        @Override
        public List<List<Value>> nextChunk() throws QsdbException {
            if (closed) {
                return null;
            }
            return reader.nextChunk();
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            resp.close(); // disconnect：传输段中止（server 停止序列化）
        }
    }

    /** 响应头 {@code x-qsdb-elapsed-ms}（服务端执行耗时；无头/解析失败 → null）。 */
    private static Double elapsedHeaderOf(Http.Response resp) {
        return elapsedHeader(resp.header("x-qsdb-elapsed-ms"));
    }

    /** 同上（流式响应形态）。 */
    private static Double elapsedHeaderOf(Http.StreamResponse resp) {
        return elapsedHeader(resp.header("x-qsdb-elapsed-ms"));
    }

    private static Double elapsedHeader(String v) {
        if (v == null || v.trim().isEmpty()) {
            return null;
        }
        try {
            return Double.valueOf(Double.parseDouble(v.trim()));
        } catch (NumberFormatException e) {
            return null; // 头形态异常按"未报告"处理，不影响主流程
        }
    }

    @Override
    public List<String> tables() throws QsdbException {
        Http.Response resp = sessionGet("/api/v1/tables", metadataReadTimeoutMs());
        if (!resp.is2xx()) {
            throw errorForStatus(resp, "tables");
        }
        Map<String, Object> root = MiniJson.parseObject(resp.text());
        Object arr = root.get("tables");
        List<String> out = new ArrayList<String>();
        if (arr instanceof List) {
            for (Object o : (List<?>) arr) {
                out.add(String.valueOf(o));
            }
        }
        return out;
    }

    @Override
    public java.util.Map<String, TableSchema> allTableSchemas() throws QsdbException {
        // J6：withSchema=1 一次返回全部表 + 列集（旧版 server 忽略该参数，
        // 返回无 columns 字段的名单——据此回退逐表拉取）
        Http.Response resp = sessionGet("/api/v1/tables?withSchema=1", metadataReadTimeoutMs());
        if (!resp.is2xx()) {
            throw errorForStatus(resp, "tables with schema");
        }
        Object arr = MiniJson.parseObject(resp.text()).get("tables");
        if (!(arr instanceof List)) {
            throw Errors.connection("unexpected response body from /api/v1/tables", null);
        }
        java.util.Map<String, TableSchema> out =
                new java.util.LinkedHashMap<String, TableSchema>();
        boolean sawAnyColumns = false;
        for (Object o : (List<?>) arr) {
            if (!(o instanceof Map)) {
                throw Errors.connection("malformed table entry in tables response", null);
            }
            Map<?, ?> t = (Map<?, ?>) o;
            String name = str(t.get("name"));
            Object colsObj = t.get("columns");
            List<TableSchema.Column> cols = new ArrayList<TableSchema.Column>();
            if (colsObj instanceof List) {
                sawAnyColumns = true;
                for (Object c : (List<?>) colsObj) {
                    if (!(c instanceof Map)) {
                        throw Errors.connection("malformed column entry in table schema", null);
                    }
                    Map<?, ?> m = (Map<?, ?>) c;
                    cols.add(new TableSchema.Column(
                            str(m.get("name")),
                            str(m.get("type")),
                            Boolean.TRUE.equals(m.get("nullable")),
                            Boolean.TRUE.equals(m.get("partition"))));
                }
            }
            out.put(name, new TableSchema(name, cols));
        }
        if (!sawAnyColumns && !out.isEmpty()) {
            // 旧 server：批量端点未生效（静默忽略参数）——回退逐表拉取
            java.util.Map<String, TableSchema> fallback =
                    new java.util.LinkedHashMap<String, TableSchema>();
            for (String name : out.keySet()) {
                TableSchema ts = tableSchema(name);
                fallback.put(name, ts);
            }
            return fallback;
        }
        return out;
    }

    /** {@code qsdb-rows} 批量通道的 Content-Type（契约，内核 docs/rows-format.md）。 */
    private static final String ROWS_CONTENT_TYPE = "application/vnd.qsdb.rows";

    @Override
    public List<PartitionInfo> tablePartitions(String database, String table) throws QsdbException {
        String encoded;
        try {
            encoded = URLEncoder.encode(table, "UTF-8").replace("+", "%20");
        } catch (java.io.UnsupportedEncodingException impossible) {
            throw new IllegalStateException(impossible); // UTF-8 恒存在
        }
        Http.Response resp = sessionGet("/api/v1/tables/" + encoded + "/partitions",
                metadataReadTimeoutMs());
        if (!resp.is2xx()) {
            throw errorForStatus(resp, "partitions of table '" + table + "'");
        }
        Map<String, Object> root = MiniJson.parseObject(resp.text());
        Object arr = root.get("detail"); // 双形态兼容：partitions 名单 + detail（含 generation）
        List<PartitionInfo> out = new ArrayList<PartitionInfo>();
        if (arr instanceof List) {
            for (Object o : (List<?>) arr) {
                if (!(o instanceof Map)) {
                    continue;
                }
                Map<?, ?> m = (Map<?, ?>) o;
                Object name = m.get("name");
                if (name == null) {
                    continue;
                }
                out.add(new PartitionInfo(String.valueOf(name), longOf(m.get("generation")),
                        longOf(m.get("rows")), longOf(m.get("segments")), longOf(m.get("bytes"))));
            }
        }
        return out;
    }

    private static long longOf(Object o) {
        return o instanceof Number ? ((Number) o).longValue() : 0L;
    }

    @Override
    public long writeBulk(String table, String mode, byte[] payload) throws QsdbException {
        return writeBulk(null, table, mode, null, payload);
    }

    @Override
    public long writeBulk(String database, String table, String mode, byte[] payload)
            throws QsdbException {
        return writeBulk(database, table, mode, null, payload);
    }

    @Override
    public long writeBulk(String database, String table, String mode, Long expectedGeneration,
                          byte[] payload) throws QsdbException {
        String encTable;
        try {
            encTable = URLEncoder.encode(table, "UTF-8").replace("+", "%20");
        } catch (java.io.UnsupportedEncodingException impossible) {
            throw new IllegalStateException(impossible);
        }
        StringBuilder path = new StringBuilder("/api/v1/write?table=").append(encTable)
                .append("&mode=").append(mode == null ? "append" : mode)
                .append("&format=rows");
        if (expectedGeneration != null) {
            // F24 CAS：generation 不匹配 → WRITE_CONFLICT（重读再决定，勿盲重试）
            path.append("&expected_generation=").append(expectedGeneration);
        }
        if (database != null && !database.isEmpty()) {
            try {
                path.append("&database=").append(
                        URLEncoder.encode(database, "UTF-8").replace("+", "%20"));
            } catch (java.io.UnsupportedEncodingException impossible) {
                throw new IllegalStateException(impossible);
            }
        }
        // 写路径：SESSION_REVOKED → 只重登不重发（副作用不可赌），原错误交回
        Http.Response resp = sessionPostBytes(path.toString(), ROWS_CONTENT_TYPE, payload,
                writeReadTimeoutMs());
        if (!resp.is2xx()) {
            throw errorForStatus(resp, "bulk write to " + table);
        }
        Map<String, Object> root = MiniJson.parseObject(resp.text());
        // 字段演进：优先结构化 rowsAffected（J12），回退 write 端点固有的 rows
        Object n = root.get("rowsAffected");
        if (!(n instanceof Number)) {
            n = root.get("rows");
        }
        return n instanceof Number ? ((Number) n).longValue() : 0L;
    }

    /** 批量写入的读超时：写路径可能长时间落盘，比元数据请求更宽。 */
    private long writeReadTimeoutMs() {
        return defaultQueryTimeoutMs > 0 ? readTimeoutFor(defaultQueryTimeoutMs) : 300_000L;
    }

    @Override
    public TableSchema tableSchema(String table) throws QsdbException {
        String encoded;
        try {
            encoded = URLEncoder.encode(table, "UTF-8").replace("+", "%20");
        } catch (java.io.UnsupportedEncodingException impossible) {
            throw new IllegalStateException(impossible); // UTF-8 恒存在
        }
        Http.Response resp = sessionGet("/api/v1/tables/" + encoded + "/schema",
                metadataReadTimeoutMs());
        if (!resp.is2xx()) {
            throw errorForStatus(resp, "schema of table '" + table + "'");
        }
        Map<String, Object> root = MiniJson.parseObject(resp.text());
        Object colsObj = root.get("columns");
        if (!(colsObj instanceof List)) {
            throw Errors.connection("unexpected response body from table schema endpoint", null);
        }
        List<TableSchema.Column> cols = new ArrayList<TableSchema.Column>();
        for (Object o : (List<?>) colsObj) {
            if (!(o instanceof Map)) {
                throw Errors.connection("malformed column entry in table schema", null);
            }
            Map<?, ?> m = (Map<?, ?>) o;
            cols.add(new TableSchema.Column(
                    str(m.get("name")),
                    str(m.get("type")),
                    Boolean.TRUE.equals(m.get("nullable")),
                    Boolean.TRUE.equals(m.get("partition"))));
        }
        return new TableSchema(str(root.get("table")), cols);
    }

    /**
     * 挂载库清单（内核十六批 M-B：{@code GET /api/v1/databases}；admin 全列、
     * 非 admin 只列有 grant 的库——与 {@code SHOW DATABASES} 同口径）。
     * 旧 server（无此端点，404）→ 抛 QsdbException，调用方自行降级。
     */
    @Override
    public List<DbInfo> databases() throws QsdbException {
        Http.Response resp = sessionGet("/api/v1/databases", metadataReadTimeoutMs());
        if (!resp.is2xx()) {
            throw errorForStatus(resp, "databases");
        }
        Map<String, Object> root = MiniJson.parseObject(resp.text());
        Object arr = root.get("databases");
        List<DbInfo> out = new ArrayList<DbInfo>();
        if (arr instanceof List) {
            for (Object o : (List<?>) arr) {
                if (!(o instanceof Map)) {
                    continue;
                }
                Map<?, ?> m = (Map<?, ?>) o;
                Object name = m.get("name");
                if (name == null) {
                    continue;
                }
                out.add(new DbInfo(String.valueOf(name),
                        Boolean.TRUE.equals(m.get("readOnly"))));
            }
        }
        return out;
    }

    @Override
    public boolean healthy(long timeoutMs) {
        try {
            fetchHealth(timeoutMs);
            return true; // JDBC isValid 语义：任何失败 → false，不抛
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String serverVersion() throws QsdbException {
        Map<String, Object> health = fetchHealth(0L);
        Object v = health.get("version");
        return v == null ? null : String.valueOf(v);
    }

    /** GET /health：返回解析后的响应体（要求 2xx 且 status=ok），否则抛 QsdbException。 */
    private Map<String, Object> fetchHealth(long timeoutMs) throws QsdbException {
        long cap = timeoutMs > 0 ? timeoutMs : 0;
        long connect = cap > 0 ? Math.min(cap, connectTimeoutMs) : connectTimeoutMs;
        Http.Response resp;
        try {
            resp = Http.get(baseUrl + "/health", effectiveToken(), connect, cap);
        } catch (IOException e) {
            throw Errors.connection("health request failed: " + e.getMessage(), e);
        }
        if (!resp.is2xx()) {
            throw errorForStatus(resp, "health");
        }
        Map<String, Object> body = MiniJson.parseObject(resp.text());
        if (!"ok".equals(body.get("status"))) {
            throw Errors.connection("health check reported status: " + body.get("status"), null);
        }
        return body;
    }

    @Override
    public void close() {
        // HTTP 无长连接资源；会话 token 随连接对象一起废弃（server 侧会话由
        // 空闲回收/上限闸管理，JDBC close 不强制踢会话——连接池借还场景下
        // 会话仍可复用）
    }

    // ------------------------------------------------------------------
    // 会话面（内核十八批 C1）：懒登录 / 单飞重登 / 读重发·写只重登
    // ------------------------------------------------------------------

    /** 当前生效的 Bearer token：会话 token 优先（Bearer 用户 token > 静态 token）。 */
    private String effectiveToken() {
        String t = sessionToken;
        return t != null ? t : staticToken;
    }

    /** 懒登录：有 login 凭证且尚无会话 token → 登录一次。 */
    private void ensureSession() throws QsdbException {
        if (loginUser != null && sessionToken == null) {
            login();
        }
    }

    /**
     * Basic → {@code POST /api/v1/auth/login} → 进程态会话 token
     * （内核十八批 C1；J9 的 v1.1 候选正式落地）。synchronized = 单飞：并发
     * 请求撞上会话失效时只放一个进去重登，其余锁内复检后直接复用新 token。
     * 登录只验凭证不判授权（授权每请求现查）。
     */
    public synchronized void login() throws QsdbException {
        if (loginUser == null) {
            throw new QsdbException(
                    "login requires both user and password connection properties", "HY000");
        }
        if (sessionToken != null) {
            return; // 锁内复检：并发 login 只做一次往返
        }
        String basic = "Basic " + java.util.Base64.getEncoder().encodeToString(
                (loginUser + ":" + loginPassword).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Http.Response resp;
        try {
            resp = Http.postJsonWithAuth(baseUrl + "/api/v1/auth/login", basic, "{}",
                    connectTimeoutMs, metadataReadTimeoutMs());
        } catch (IOException e) {
            throw Errors.connection("login request failed: " + e.getMessage(), e);
        }
        if (!resp.is2xx()) {
            throw errorForStatus(resp, "login");
        }
        Map<String, Object> body = MiniJson.parseObject(resp.text());
        Object token = body.get("token");
        if (!(token instanceof String) || ((String) token).isEmpty()) {
            throw Errors.connection("login response missing token", null);
        }
        sessionToken = (String) token;
        sessionId = str(body.get("sessionId"));
        sessionRole = str(body.get("role"));
        sessionDefaultDb = str(body.get("defaultDb"));
    }

    /** 会话是否处于登录态（会话面可用；静态 token / 匿名连接为 false）。 */
    public boolean isSessionLogin() {
        return sessionToken != null;
    }

    /** 登录会话 id（内核十八批 C1；未登录为 null）。 */
    public String sessionId() {
        return sessionId;
    }

    /** 登录用户的角色（{@code admin}/{@code writer}/{@code reader}；未登录为 null）。 */
    public String sessionRole() {
        return sessionRole;
    }

    /** 登录回执的会话默认库（未登录 / 旧 server 为 null）。 */
    public String sessionDefaultDb() {
        return sessionDefaultDb;
    }

    /**
     * 响应是否为 401 {@code SESSION_REVOKED}（会话被踢/重启/空闲回收——
     * 错误以响应形态回来，code 在错误体里，J3 契约）。
     */
    private boolean isRevokedResponse(Http.Response resp) {
        if (resp.is2xx() || loginUser == null) {
            return false;
        }
        return "SESSION_REVOKED".equals(extractErrorCode(resp));
    }

    /**
     * 带会话的 GET（读语义）：撞 401 {@code SESSION_REVOKED} 且持有登录凭证
     * → <b>单飞重登后原样重发一次</b>（安全依据：认证闸在执行前，401 = 请求
     * 从未生效）。
     */
    private Http.Response sessionGet(String path, long readTimeoutMs) throws QsdbException {
        ensureSession();
        Http.Response resp;
        try {
            resp = Http.get(baseUrl + path, effectiveToken(), connectTimeoutMs, readTimeoutMs);
        } catch (IOException e) {
            throw Errors.connection("request failed: " + e.getMessage(), e);
        }
        if (!isRevokedResponse(resp)) {
            return resp;
        }
        sessionToken = null;
        login(); // 单飞重登（synchronized；并发失败只做一次往返）
        try {
            return Http.get(baseUrl + path, effectiveToken(), connectTimeoutMs, readTimeoutMs);
        } catch (IOException ioe) {
            throw Errors.connection("request failed after relogin: " + ioe.getMessage(), ioe);
        }
    }

    /**
     * 带会话的 POST JSON（/query 主通道；含 DML——重发安全性同上：401 =
     * 认证闸在执行前，语句从未运行）。
     */
    private Http.Response sessionPostJson(String path, Map<String, String> headers, String body,
                                          long connectMs, long readTimeoutMs) throws QsdbException {
        ensureSession();
        Http.Response resp;
        try {
            resp = Http.postJson(baseUrl + path, effectiveToken(), headers, body,
                    connectMs, readTimeoutMs);
        } catch (IOException e) {
            throw Errors.connection("request failed: " + e.getMessage(), e);
        }
        if (!isRevokedResponse(resp)) {
            return resp;
        }
        sessionToken = null;
        login();
        try {
            return Http.postJson(baseUrl + path, effectiveToken(), headers, body,
                    connectMs, readTimeoutMs);
        } catch (IOException ioe) {
            throw Errors.connection("request failed after relogin: " + ioe.getMessage(), ioe);
        }
    }

    /**
     * 带会话的写调用（/write 二进制通道）：revoked → <b>只重登、不重发</b>
     * （对齐 qsdb-client 写路径纪律——副作用不可赌：写可能已生效）。重登后
     * 原样返回该 401 响应，由调用方的 {@code errorForStatus} 映射上抛——
     * 应用层直接重试即可成功。
     */
    private Http.Response sessionPostBytes(String path, String contentType, byte[] body,
                                           long readTimeoutMs) throws QsdbException {
        ensureSession();
        Http.Response resp;
        try {
            resp = Http.postBytes(baseUrl + path, effectiveToken(), contentType, body,
                    connectTimeoutMs, readTimeoutMs);
        } catch (IOException e) {
            throw Errors.connection("write request failed: " + e.getMessage(), e);
        }
        if (isRevokedResponse(resp)) {
            sessionToken = null;
            login(); // 只重登不重发
        }
        return resp;
    }

    // ------------------------------------------------------------------

    /** server 侧查询超时 → 客户端读超时（+余量，保证 server 先返回）。 */
    private long readTimeoutFor(long queryTimeoutMs) {
        return queryTimeoutMs > 0 ? queryTimeoutMs + READ_TIMEOUT_SLACK_MS : 0L;
    }

    /** 元数据请求的读超时：随查询超时策略，未配置时给 60s 上限防挂死。 */
    private long metadataReadTimeoutMs() {
        return defaultQueryTimeoutMs > 0 ? readTimeoutFor(defaultQueryTimeoutMs) : 60_000L;
    }

    /**
     * HTTP 状态 + 结构化错误码 → QsdbException。
     *
     * <p>映射链（2026-10-12 码表体系对账）：code → 运行期码表
     * （{@code GET /api/v1/error-codes}，懒拉取一次）→ 本地兜底表
     * {@link ErrorCodeTable} → 旧版状态码回退。SQLState 归驱动所有：
     * 绝大多数码采用内核建议；QUERY_TIMEOUT 恒 HYT00（§十 契约）。
     * 异常 message 携带 {@code [code=…, retry=…]} 供上层分流
     * （如 TOO_MANY_QUERIES 可退避、READ_ONLY 应换实例）。
     */
    private QsdbException errorForStatus(Http.Response resp, String what) {
        String serverMsg = extractErrorMessage(resp);
        String code = extractErrorCode(resp);
        if (code != null) {
            ErrorCodeTable.Entry entry = resolveEntry(code);
            String sqlState = entry != null ? entry.sqlState : null;
            boolean retryable = entry != null && entry.retryable;
            String annotated = serverMsg + " [code=" + code
                    + (entry != null ? ", retry=" + (retryable ? "yes" : "no") : "")
                    + "] (HTTP " + resp.status + ")";
            if ("QUERY_TIMEOUT".equals(code)) {
                return Errors.timeout(annotated, code); // HYT00（§十 契约）
            }
            if (sqlState == null) {
                // 未知码（本地表与运行期表都没有）→ 按状态码归类，保底不崩
                return classifyByStatus(serverMsg + " [code=" + code + "] (HTTP "
                        + resp.status + ")", resp.status);
            }
            if (sqlState.startsWith("28")) {
                return Errors.auth(serverMsg, resp.status, code); // UNAUTHORIZED 与 SESSION_REVOKED 靠 code 分流
            }
            if (sqlState.startsWith("08")) {
                // 08S01/08P01/58030：连接/IO 层——IO_ERROR retry=yes 提示在 message
                return Errors.connection(annotated, null, code);
            }
            if ("0A000".equals(sqlState)) {
                return new QsdbException(annotated, "0A000", Errors.unsupported(serverMsg), code);
            }
            // 语义错误族（42xxx/42601/53200/25006/XX000/HY008…）
            return new QsdbException(annotated, sqlState, null, code);
        }
        // ---- 旧版回退：无 code 字段（J3 之前的 server）----
        return classifyByStatus(serverMsg, resp.status);
    }

    /** 旧版回退：按状态码 + 消息前缀归类（无结构化 code 的 server）。 */
    private QsdbException classifyByStatus(String serverMsg, int status) {
        switch (status) {
            case 400:
            case 404:
            case 409:
            case 422:
                return Errors.syntax(serverMsg, status);
            case 401:
            case 403:
                return Errors.auth(serverMsg, status);
            case 501:
                return new QsdbException(serverMsg, "0A000", Errors.unsupported(serverMsg));
            default:
                if (status >= 500) {
                    // 旧内核查询超时走 500 无专用码——按消息前缀识别
                    if (serverMsg.startsWith("query timeout")) {
                        return Errors.timeout(serverMsg + " (HTTP " + status + ")");
                    }
                    return Errors.connection(serverMsg + " (HTTP " + status + ")", null);
                }
                return Errors.syntax(serverMsg, status);
        }
    }

    /**
     * code → 条目：运行期码表优先（内核加码自动跟上），本地兜底表次之。
     * QUERY_TIMEOUT 恒 HYT00（驱动契约覆盖内核建议 57014）。
     */
    private ErrorCodeTable.Entry resolveEntry(String code) {
        Map<String, ErrorCodeTable.Entry> dyn = dynamicCodes();
        ErrorCodeTable.Entry e = dyn.get(code);
        if (e == null) {
            e = ErrorCodeTable.lookup(code);
        }
        if (e != null && "QUERY_TIMEOUT".equals(code)) {
            return new ErrorCodeTable.Entry("HYT00", e.retryable);
        }
        return e;
    }

    /**
     * 懒拉取运行期码表（一次；失败静默降级本地表，不重复尝试——错误路径
     * 上的额外往返只付出一次）。免鉴权端点，无需 token。
     */
    private Map<String, ErrorCodeTable.Entry> dynamicCodes() {
        Map<String, ErrorCodeTable.Entry> cached = dynamicCodes;
        if (cached != null) {
            return cached;
        }
        Map<String, ErrorCodeTable.Entry> parsed = null;
        try {
            Http.Response resp = Http.get(baseUrl + "/api/v1/error-codes", effectiveToken(),
                    connectTimeoutMs, metadataReadTimeoutMs());
            if (resp.is2xx()) {
                Object arr = MiniJson.parseObject(resp.text()).get("codes");
                if (arr instanceof List) {
                    parsed = new HashMap<String, ErrorCodeTable.Entry>();
                    for (Object o : (List<?>) arr) {
                        if (!(o instanceof Map)) {
                            continue;
                        }
                        Map<?, ?> m = (Map<?, ?>) o;
                        Object code = m.get("code");
                        Object sqlState = m.get("sqlState");
                        if (code == null || sqlState == null) {
                            continue;
                        }
                        parsed.put(String.valueOf(code),
                                new ErrorCodeTable.Entry(String.valueOf(sqlState),
                                        Boolean.TRUE.equals(m.get("retry"))));
                    }
                }
            }
        } catch (Exception ignored) {
            parsed = null; // 码表不可得——本地兜底
        }
        Map<String, ErrorCodeTable.Entry> effective =
                parsed == null ? Collections.<String, ErrorCodeTable.Entry>emptyMap() : parsed;
        dynamicCodes = effective;
        return effective;
    }

    /**
     * 精确取消当前正在执行的查询（J11）：query 请求经 {@code X-QSDB-Query-Id}
     * 头预注册了客户端 id，本方法直取该 id 调 {@code POST /queries/{id}/cancel}
     * ——零扫描、零误伤。返回 false 表示没有可取消目标（无执行中查询，或
     * server 未采用客户端 id / 查询已结束），调用方可回退
     * {@link #cancelQueriesMatching}。
     */
    @Override
    public boolean cancelActiveQuery() throws QsdbException {
        String id = inFlightQueryId.get();
        if (id == null) {
            return false;
        }
        Http.Response cancel = sessionPostJson("/api/v1/queries/" + id + "/cancel", null, "",
                connectTimeoutMs, metadataReadTimeoutMs());
        if (cancel.is2xx()) {
            return true;
        }
        // 404 = 查询已结束或 server 未注册该 id（旧版/未采用客户端 id）→ 回退信号
        if (cancel.status == 404) {
            return false;
        }
        throw errorForStatus(cancel, "cancel of query " + id);
    }

    /**
     * 取消与给定 SQL 全等（trim 后）的运行中查询（回退路径：server 未采用
     * 客户端 id 或 cancel 目标 404 时使用）。按 SQL 匹配——同 SQL 并发
     * 执行时会一并取消（含其它连接的），故仅在精确取消不可用时作为兜底。
     */
    @Override
    public void cancelQueriesMatching(String sql) throws QsdbException {
        Http.Response list = sessionGet("/api/v1/queries", metadataReadTimeoutMs());
        if (!list.is2xx()) {
            throw errorForStatus(list, "query list");
        }
        Object arr = MiniJson.parseObject(list.text()).get("queries");
        if (!(arr instanceof List)) {
            return;
        }
        String target = sql == null ? "" : sql.trim();
        for (Object o : (List<?>) arr) {
            if (!(o instanceof Map)) {
                continue;
            }
            Map<?, ?> q = (Map<?, ?>) o;
            Object qsql = q.get("sql");
            if (qsql == null || !target.equals(String.valueOf(qsql).trim())) {
                continue;
            }
            Object id = q.get("id");
            if (id == null) {
                continue;
            }
            Http.Response cancel = sessionPostJson("/api/v1/queries/" + id + "/cancel", null, "",
                    connectTimeoutMs, metadataReadTimeoutMs());
            if (!cancel.is2xx()) {
                throw errorForStatus(cancel, "cancel of query " + id);
            }
        }
    }

    /** 从错误响应体提取 {@code {"error": …}}；解析失败退化为原文截断。 */
    private String extractErrorMessage(Http.Response resp) {
        String body = resp.text();
        try {
            Object err = MiniJson.parseObject(body).get("error");
            if (err != null) {
                return String.valueOf(err);
            }
        } catch (QsdbException ignored) {
            // 非 JSON 响应体——用原文
        }
        String trimmed = body.trim();
        return trimmed.isEmpty() ? "server returned HTTP " + resp.status : trimmed;
    }

    /**
     * 从错误响应体提取结构化错误码（内核 J3 契约：{@code {"error","code"}}，
     * code 契约级稳定）。旧版 server 无 code 字段 → null（回退到状态码/前缀映射）。
     */
    private String extractErrorCode(Http.Response resp) {
        try {
            Object code = MiniJson.parseObject(resp.text()).get("code");
            return code == null ? null : String.valueOf(code);
        } catch (QsdbException ignored) {
            return null;
        }
    }

    private QueryResponse parseQueryResponse(List<?> schema, Object rowsObj, Double elapsedMs,
                                             String queryId) throws QsdbException {
        List<ColMeta> cols = new ArrayList<ColMeta>();
        for (Object o : schema) {
            if (!(o instanceof Map)) {
                throw Errors.connection("malformed column entry in query response schema", null);
            }
            Map<?, ?> m = (Map<?, ?>) o;
            cols.add(new ColMeta(str(m.get("name")), str(m.get("type"))));
        }
        List<List<Value>> rows = new ArrayList<List<Value>>();
        if (rowsObj instanceof List) {
            for (Object rowObj : (List<?>) rowsObj) {
                if (!(rowObj instanceof List)) {
                    throw Errors.connection("malformed row in query response", null);
                }
                List<?> rawRow = (List<?>) rowObj;
                if (rawRow.size() != cols.size()) {
                    throw Errors.connection("row width " + rawRow.size()
                            + " does not match schema width " + cols.size(), null);
                }
                List<Value> row = new ArrayList<Value>(rawRow.size());
                for (Object cell : rawRow) {
                    row.add(toValue(cell));
                }
                rows.add(java.util.Collections.unmodifiableList(row));
            }
        }
        return QueryResponse.ofQuery(cols, rows, elapsedMs, queryId);
    }

    /** JSON 单元格 → Value（内核值形态：null/整数/浮点/布尔/字符串）。 */
    private static Value toValue(Object cell) throws QsdbException {
        if (cell == null) {
            return Value.NULL;
        }
        if (cell instanceof Long) {
            return Value.of(((Long) cell).longValue());
        }
        if (cell instanceof Double) {
            return Value.of(((Double) cell).doubleValue());
        }
        if (cell instanceof Boolean) {
            return Value.of(((Boolean) cell).booleanValue());
        }
        if (cell instanceof String) {
            return Value.of((String) cell);
        }
        throw Errors.connection("unsupported cell value type " + cell.getClass().getSimpleName(), null);
    }

    /** body 的 elapsedMs 字段（旧 server 路径）；缺省/形态异常 → null。 */
    private static Double elapsedOf(Object o) {
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        return null;
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }
}
