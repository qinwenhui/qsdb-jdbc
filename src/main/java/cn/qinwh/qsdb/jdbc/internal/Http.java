package cn.qinwh.qsdb.jdbc.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * 内建 HTTP 客户端：{@code HttpURLConnection} 的薄封装（设计文档 §十一）。
 *
 * <p>连接复用交给 JDK keep-alive 连接池（{@code http.keepAlive} 默认开），
 * 驱动不自建池。为使 keep-alive 生效，响应体必须读尽后关闭流——本类已处理。
 *
 * <p>认证：{@code Authorization: Bearer <token>}（token 为 null 时省略——
 * server 开放模式）。token 只经请求头传输，不出现在 URL 与日志。
 *
 * <p>internal API——不在驱动公开面上。
 */
public final class Http {

    private Http() {
    }

    /** 响应：状态码 + 响应头 + 已读尽的 UTF-8 响应体。 */
    public static final class Response {
        public final int status;
        /** 原始响应字节——二进制通道（rows/bulk）必须按字节处理（UTF-8 解码会损坏数据）。 */
        public final byte[] bytes;
        /** 响应头（键小写、取首个值；多值头取逗号拼接语义不保——当前契约无此需求）。 */
        private final Map<String, String> headers;
        private String text; // 文本形态（懒解码并缓存）

        Response(int status, byte[] bytes, Map<String, String> headers) {
            this.status = status;
            this.bytes = bytes;
            this.headers = headers == null
                    ? Collections.<String, String>emptyMap()
                    : headers;
        }

        /** 响应头（键名大小写不敏感；无该头返回 null）。 */
        public String header(String name) {
            return name == null ? null : headers.get(name.toLowerCase(java.util.Locale.ROOT));
        }

        /** 响应体的 UTF-8 文本形态（JSON/错误体路径用；懒解码）。 */
        public String text() {
            if (text == null) {
                text = new String(bytes, StandardCharsets.UTF_8);
            }
            return text;
        }

        public boolean is2xx() {
            return status >= 200 && status < 300;
        }

        /** 工厂：从已读尽的字节构造（流式响应的错误体走错误映射时用）。 */
        public static Response of(int status, byte[] bytes) {
            return new Response(status, bytes, Collections.<String, String>emptyMap());
        }
    }

    /** GET 请求。 */
    public static Response get(String url, String token, long connectTimeoutMs, long readTimeoutMs)
            throws IOException {
        return exchange("GET", url, token, null, null, connectTimeoutMs, readTimeoutMs);
    }

    /** POST JSON 请求。 */
    public static Response postJson(String url, String token, String jsonBody,
                                    long connectTimeoutMs, long readTimeoutMs) throws IOException {
        return postJson(url, token, null, jsonBody, connectTimeoutMs, readTimeoutMs);
    }

    /** POST JSON 请求（附加请求头，如 {@code X-QSDB-Query-Id}）。 */
    public static Response postJson(String url, String token, java.util.Map<String, String> extraHeaders,
                                    String jsonBody, long connectTimeoutMs, long readTimeoutMs)
            throws IOException {
        return exchange("POST", url, token, extraHeaders, jsonBody,
                "application/json", connectTimeoutMs, readTimeoutMs);
    }

    /**
     * POST JSON 请求，Authorization 头<b>显式给定</b>（login 端点用：必须带
     * Basic 凭证，不吃当前 token——内核十八批 C1 契约）。
     */
    public static Response postJsonWithAuth(String url, String authorizationHeader,
                                            String jsonBody, long connectTimeoutMs, long readTimeoutMs)
            throws IOException {
        return exchangeWithAuth("POST", url, authorizationHeader, null, jsonBody,
                "application/json", connectTimeoutMs, readTimeoutMs);
    }

    /**
     * POST 原始字节体（批量通道用：{@code application/vnd.qsdb.rows} 等）。
     * 与 JSON 路径共用连接池与超时语义。
     */
    public static Response postBytes(String url, String token, String contentType, byte[] body,
                                     long connectTimeoutMs, long readTimeoutMs) throws IOException {
        return exchange("POST", url, token, null, body, contentType, connectTimeoutMs, readTimeoutMs);
    }

    // ------------------------------------------------------------------
    // 流式响应（P3：setFetchSize 真流式——rows 通道 + Connection: close）
    // ------------------------------------------------------------------

    /**
     * 流式响应：状态码/头立即可读，响应体以 {@link InputStream} 渐进消费。
     * 与 {@link Response}（全量读尽）相对——大结果集不再整包进内存。
     *
     * <p><b>资源纪律</b>：2xx 时必须 {@link #close()}（读尽后关流保 JDK
     * keep-alive；提前关闭=中断传输——server 侧 socket 关闭即停止序列化）；
     * 请求带 {@code Connection: close}，本连接不复用（内核流式契约）。
     */
    public static final class StreamResponse {
        public final int status;
        private final Map<String, String> headers;
        private final HttpURLConnection conn;
        private final InputStream body;
        private final byte[] errorBody; // 非 2xx 时已读尽的错误体

        StreamResponse(int status, Map<String, String> headers, HttpURLConnection conn,
                       InputStream body, byte[] errorBody) {
            this.status = status;
            this.headers = headers == null ? Collections.<String, String>emptyMap() : headers;
            this.conn = conn;
            this.body = body;
            this.errorBody = errorBody;
        }

        /** 响应头（键名大小写不敏感）。 */
        public String header(String name) {
            return name == null ? null : headers.get(name.toLowerCase(java.util.Locale.ROOT));
        }

        /** 2xx 响应体流（调用方消费完负责 {@link #close()}）。 */
        public InputStream body() {
            return body;
        }

        /** 非 2xx 错误体（已读尽的原始字节）。 */
        public byte[] errorBytes() {
            return errorBody == null ? new byte[0] : errorBody;
        }

        public boolean is2xx() {
            return status >= 200 && status < 300;
        }

        /** 关闭底层连接（提前关闭 = 传输段中止——server 停止序列化）。 */
        public void close() {
            try {
                if (body != null) {
                    body.close();
                }
            } catch (IOException ignored) {
                // 关闭失败无补救
            }
            if (conn != null) {
                conn.disconnect(); // Connection: close 语义：强制断链（中止信号）
            }
        }
    }

    /**
     * POST JSON 并以流式形态返回响应（内核 rows 流式契约：请求带
     * {@code Connection: close}，响应体 {@code Content-Length} 可缺省/分块）。
     * 与 {@link #postJson} 相对——那个全量读尽，本方法交给调用方渐进消费。
     */
    public static StreamResponse postStream(String url, String token,
                                            java.util.Map<String, String> extraHeaders,
                                            String jsonBody, long connectTimeoutMs,
                                            long readTimeoutMs) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout((int) Math.max(0, Math.min(connectTimeoutMs, Integer.MAX_VALUE)));
        conn.setReadTimeout((int) Math.max(0, Math.min(readTimeoutMs, Integer.MAX_VALUE)));
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Connection", "close"); // 流式不复用（内核 query_stream 契约）
        if (token != null && !token.isEmpty()) {
            conn.setRequestProperty("Authorization", "Bearer " + token);
        }
        if (extraHeaders != null) {
            for (java.util.Map.Entry<String, String> h : extraHeaders.entrySet()) {
                conn.setRequestProperty(h.getKey(), h.getValue());
            }
        }
        conn.setDoOutput(true);
        byte[] payload = jsonBody.getBytes(StandardCharsets.UTF_8);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setFixedLengthStreamingMode(payload.length);
        try (InputStream in = new java.io.ByteArrayInputStream(payload)) {
            copy(in, conn.getOutputStream());
        }
        int status = conn.getResponseCode();
        Map<String, String> headers = headersOf(conn);
        if (status >= 400) {
            InputStream err = conn.getErrorStream();
            byte[] errorBody = err == null ? new byte[0] : readAllBytes(err);
            return new StreamResponse(status, headers, conn, null, errorBody);
        }
        return new StreamResponse(status, headers, conn, conn.getInputStream(), null);
    }

    private static Response exchange(String method, String url, String token,
                                     java.util.Map<String, String> extraHeaders, String jsonBody,
                                     long connectTimeoutMs, long readTimeoutMs) throws IOException {
        return exchange(method, url, token, extraHeaders, jsonBody,
                jsonBody == null ? null : "application/json", connectTimeoutMs, readTimeoutMs);
    }

    private static Response exchange(String method, String url, String token,
                                     java.util.Map<String, String> extraHeaders, Object body,
                                     String contentType, long connectTimeoutMs, long readTimeoutMs)
            throws IOException {
        return exchangeWithAuth(method, url,
                token == null || token.isEmpty() ? null : "Bearer " + token,
                extraHeaders, body, contentType, connectTimeoutMs, readTimeoutMs);
    }

    private static Response exchangeWithAuth(String method, String url, String authorizationHeader,
                                             java.util.Map<String, String> extraHeaders, Object body,
                                             String contentType, long connectTimeoutMs, long readTimeoutMs)
            throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout((int) Math.max(0, Math.min(connectTimeoutMs, Integer.MAX_VALUE)));
        conn.setReadTimeout((int) Math.max(0, Math.min(readTimeoutMs, Integer.MAX_VALUE)));
        conn.setRequestProperty("Accept", "application/json");
        if (authorizationHeader != null && !authorizationHeader.isEmpty()) {
            conn.setRequestProperty("Authorization", authorizationHeader);
        }
        if (extraHeaders != null) {
            for (java.util.Map.Entry<String, String> h : extraHeaders.entrySet()) {
                conn.setRequestProperty(h.getKey(), h.getValue());
            }
        }
        if (body != null) {
            conn.setDoOutput(true);
            byte[] payload = body instanceof byte[]
                    ? (byte[]) body
                    : String.valueOf(body).getBytes(StandardCharsets.UTF_8);
            if (contentType != null) {
                conn.setRequestProperty("Content-Type", contentType);
            }
            conn.setFixedLengthStreamingMode(payload.length);
            try (InputStream in = new java.io.ByteArrayInputStream(payload)) {
                copy(in, conn.getOutputStream());
            }
        }
        int status = conn.getResponseCode();
        InputStream stream = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
        byte[] responseBytes = stream == null ? new byte[0] : readAllBytes(stream);
        return new Response(status, responseBytes, headersOf(conn));
        // 注意：不调 disconnect()——响应体读尽 + 流关闭后 JDK 会把连接放回
        // keep-alive 池；disconnect 会强制断链，逐请求 TCP 握手（§十一）。
    }

    /** 捕获响应头（键小写；契约关心的头如 {@code x-qsdb-elapsed-ms}）。 */
    private static Map<String, String> headersOf(HttpURLConnection conn) {
        java.util.Map<String, java.util.List<String>> raw = conn.getHeaderFields();
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> out = new java.util.LinkedHashMap<String, String>();
        for (java.util.Map.Entry<String, java.util.List<String>> e : raw.entrySet()) {
            String k = e.getKey() == null ? null : e.getKey().toLowerCase(java.util.Locale.ROOT);
            if (k == null || e.getValue() == null || e.getValue().isEmpty()) {
                continue;
            }
            out.put(k, e.getValue().get(0));
        }
        return out;
    }

    private static void copy(InputStream in, java.io.OutputStream out) throws IOException {
        byte[] buf = new byte[8192];
        try (InputStream src = in; java.io.OutputStream dst = out) {
            int n;
            while ((n = src.read(buf)) > 0) {
                dst.write(buf, 0, n);
            }
        }
    }

    private static byte[] readAllBytes(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        try (InputStream src = in) {
            int n;
            while ((n = src.read(buf)) > 0) {
                out.write(buf, 0, n);
            }
        }
        return out.toByteArray();
    }
}
