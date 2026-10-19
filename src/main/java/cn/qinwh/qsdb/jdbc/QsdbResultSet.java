package cn.qinwh.qsdb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.math.RoundingMode;
import java.net.URI;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.qinwh.qsdb.jdbc.internal.Errors;
import cn.qinwh.qsdb.jdbc.internal.TypeStrings;
import cn.qinwh.qsdb.jdbc.transport.ColMeta;
import cn.qinwh.qsdb.jdbc.transport.QueryResponse;
import cn.qinwh.qsdb.jdbc.transport.RowsStream;
import cn.qinwh.qsdb.jdbc.transport.Value;

/**
 * 只进只读结果集（TYPE_FORWARD_ONLY / CONCUR_READ_ONLY，设计文档 §六.3）。
 *
 * <p>语义要点：
 * <ul>
 *   <li>任何 {@code getXxx} 读到 SQL NULL：返回该类型 JDBC 规范零值
 *       （0 / false / null），随后一次 {@link #wasNull()} 返回 true；</li>
 *   <li>类型转换<b>按列的 {@code QsdbType}</b> 而非值的 Java 形态——内核
 *       JSON 通道有数字启发（DOUBLE 列的整值以 long 到达、VARCHAR 列的
 *       数值串以 long 到达），值驱动会出错；</li>
 *   <li>{@code maxRows} 驱动侧截断：读到 n 行即结束（§十一，不做 SQL
 *       LIMIT 改写）；流式结果集读到上限即<b>关流断链</b>（剩余数据不再
 *       传输）；</li>
 *   <li>流式（P3，{@code setFetchSize(n>0)}）：行按块懒拉取，内存 O(一块)；
 *       提前 {@code close()} 或 {@code Statement.cancel()} = 传输段中止，
 *       已收行保留为只读数据；</li>
 *   <li>DECIMAL 的 {@code getBigDecimal} 用 {@code BigDecimal.valueOf(double)}
 *       ——JSON 通道上 DECIMAL 经 f64 中转，{@code new BigDecimal(double)}
 *       会展开二进制尾（§5.4）。</li>
 * </ul>
 */
final class QsdbResultSet implements ResultSet {

    private final QsdbStatement statement; // 元数据结果集为 null
    private final List<ColMeta> columns;
    private final TypeStrings.Parsed[] colTypes;
    private final List<List<Value>> rows;   // 流式时为渐进填充的可变列表
    private final long maxRows;
    private final RowsStream stream;        // 非 null = 流式源（P3）
    private final Map<String, Integer> indexByLowerName = new HashMap<String, Integer>();

    private int position = -1; // -1 = before first
    private List<Value> current;
    private boolean wasNullFlag;
    private boolean closed;
    private boolean streamExhausted;

    /** 查询结果集（由 QsdbStatement 创建）。 */
    QsdbResultSet(QueryResponse resp, long maxRows, QsdbStatement statement) throws SQLException {
        this(resp, maxRows, statement, true);
    }

    /** 流式结果集（P3：行按块懒拉取）。 */
    QsdbResultSet(RowsStream stream, long maxRows, QsdbStatement statement) throws SQLException {
        this.statement = statement;
        this.columns = stream.columns();
        this.rows = new ArrayList<List<Value>>();
        this.maxRows = maxRows;
        this.stream = stream;
        this.colTypes = new TypeStrings.Parsed[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            colTypes[i] = TypeStrings.parse(columns.get(i).getTypeName());
        }
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).getName();
            if (name != null) {
                indexByLowerName.put(name.toLowerCase(java.util.Locale.ROOT), i);
            }
        }
    }

    /** 内存行集（DatabaseMetaData 构造工具用）。 */
    static QsdbResultSet forRows(List<ColMeta> cols, List<List<Value>> rows) throws SQLException {
        return new QsdbResultSet(QueryResponse.ofQuery(cols, rows, null, null), 0L, null, true);
    }

    private QsdbResultSet(QueryResponse resp, long maxRows, QsdbStatement statement,
                          boolean parseTypes) throws SQLException {
        this.statement = statement;
        this.columns = resp.getColumns();
        this.rows = resp.getRows();
        this.maxRows = maxRows;
        this.stream = null;
        this.colTypes = new TypeStrings.Parsed[columns.size()];
        if (parseTypes) {
            for (int i = 0; i < columns.size(); i++) {
                colTypes[i] = TypeStrings.parse(columns.get(i).getTypeName());
            }
        }
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).getName();
            if (name != null) {
                indexByLowerName.put(name.toLowerCase(java.util.Locale.ROOT), i);
            }
        }
    }

    // ------------------------------------------------------------------
    // 游标
    // ------------------------------------------------------------------

    @Override
    public boolean next() throws SQLException {
        ensureOpen();
        int nextIdx = position + 1;
        if (stream != null && !streamExhausted && nextIdx >= rows.size()) {
            // 到达 maxRows 上限即不再拉取——关流断链（剩余传输作废）
            if (maxRows > 0 && nextIdx >= maxRows) {
                abortStream();
            } else {
                pullOneChunk();
            }
        }
        long limit = maxRows > 0 ? Math.min((long) rows.size(), maxRows) : rows.size();
        if (nextIdx < limit) {
            position = nextIdx;
            current = rows.get(position);
            wasNullFlag = false;
            return true;
        }
        position = (int) limit; // afterLast
        current = null;
        return false;
    }

    /** 拉取下一块（一次一块——消费节奏由调用方 next() 驱动）。 */
    private void pullOneChunk() throws SQLException {
        try {
            List<List<Value>> chunk = stream.nextChunk();
            if (chunk == null) {
                streamExhausted = true;
                stream.close(); // 干净到尾：释放连接
            } else {
                rows.addAll(chunk);
            }
        } catch (cn.qinwh.qsdb.jdbc.QsdbException e) {
            streamExhausted = true;
            throw e; // HY000 透传（流中断是真实故障，不掩盖）
        }
    }

    /**
     * 传输段中止（{@code Statement.cancel()} / 截断到 maxRows）：关流断链，
     * 已收行保留为只读数据，迭代自然结束（对齐 Studio/内核两段停止语义）。
     */
    void abortStream() {
        if (stream != null && !streamExhausted) {
            streamExhausted = true;
            stream.close();
        }
    }

    @Override
    public void close() {
        closed = true;
        current = null;
        if (stream != null) {
            abortStream(); // 幂等；干净到尾时为 no-op
        }
        if (statement != null) {
            statement.resultSetClosed(this);
        }
    }

    @Override
    public boolean wasNull() {
        return wasNullFlag;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public int getRow() {
        return position + 1; // afterLast 时返回 rowcount+1，与规范“当前行号”语义一致
    }

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw Errors.invalid("result set is closed");
        }
    }

    private void ensureOnRow() throws SQLException {
        ensureOpen();
        if (position < 0 || position >= rows.size() || current == null) {
            throw Errors.invalid("cursor is not positioned on a row (call next() first)");
        }
    }

    // ------------------------------------------------------------------
    // 列定位
    // ------------------------------------------------------------------

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        ensureOpen();
        for (int i = 0; i < columns.size(); i++) {
            if (columnLabel.equals(columns.get(i).getName())) {
                return i + 1;
            }
        }
        Integer idx = indexByLowerName.get(columnLabel.toLowerCase(java.util.Locale.ROOT));
        if (idx != null) {
            return idx + 1;
        }
        throw Errors.invalid("column not found: '" + columnLabel + "'");
    }

    private Value cell(int columnIndex) throws SQLException {
        ensureOnRow();
        int idx = columnIndex - 1;
        if (idx < 0 || idx >= current.size()) {
            throw Errors.invalid("column index " + columnIndex + " out of range 1.." + current.size());
        }
        Value v = current.get(idx);
        wasNullFlag = v.isNull();
        return v;
    }

    private TypeStrings.Parsed colType(int columnIndex) throws SQLException {
        int idx = columnIndex - 1;
        if (idx < 0 || idx >= colTypes.length) {
            throw Errors.invalid("column index " + columnIndex + " out of range 1.." + colTypes.length);
        }
        return colTypes[idx];
    }

    // ------------------------------------------------------------------
    // 数值族
    // ------------------------------------------------------------------

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        long v = getLong(columnIndex);
        if (wasNullFlag) {
            return 0;
        }
        if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
            throw Errors.numericOutOfRange("value " + v + " does not fit in BYTE");
        }
        return (byte) v;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        long v = getLong(columnIndex);
        if (wasNullFlag) {
            return 0;
        }
        if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
            throw Errors.numericOutOfRange("value " + v + " does not fit in SHORT");
        }
        return (short) v;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        long v = getLong(columnIndex);
        if (wasNullFlag) {
            return 0;
        }
        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
            throw Errors.numericOutOfRange("value " + v + " does not fit in INT");
        }
        return (int) v;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return 0L;
        }
        switch (v.getKind()) {
            case LONG:
                return v.asLong();
            case DOUBLE:
                double d = v.asDouble();
                if (Double.isNaN(d)) {
                    throw Errors.conversion("cannot convert NaN to integer value");
                }
                if (d >= 9.223372036854776E18 || d <= -9.223372036854776E18) {
                    throw Errors.numericOutOfRange("double value " + d + " does not fit in BIGINT");
                }
                return (long) d;
            case BOOL:
                return v.asBoolean() ? 1L : 0L;
            case STRING:
                return parseLong(v.asString().trim());
            default:
                throw Errors.conversion("cannot convert " + v.getKind() + " value to long");
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return (float) getDouble(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return 0d;
        }
        switch (v.getKind()) {
            case LONG:
                return v.asLong();
            case DOUBLE:
                return v.asDouble();
            case BOOL:
                return v.asBoolean() ? 1d : 0d;
            case STRING:
                return parseDouble(v.asString().trim());
            default:
                throw Errors.conversion("cannot convert " + v.getKind() + " value to double");
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bd = getBigDecimal(columnIndex);
        if (wasNullFlag) {
            return null;
        }
        return bd.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return null;
        }
        switch (v.getKind()) {
            case LONG:
                return BigDecimal.valueOf(v.asLong());
            case DOUBLE:
                // §5.4：JSON 通道 DECIMAL 经 f64 中转——valueOf 恢复到可表达精度，
                // new BigDecimal(double) 会展开二进制尾（评审红线）
                return BigDecimal.valueOf(v.asDouble());
            case STRING:
                try {
                    return new BigDecimal(v.asString().trim());
                } catch (NumberFormatException e) {
                    throw Errors.conversion("cannot convert '" + v.asString() + "' to BigDecimal");
                }
            default:
                throw Errors.conversion("cannot convert " + v.getKind() + " value to BigDecimal");
        }
    }

    // ------------------------------------------------------------------
    // 布尔
    // ------------------------------------------------------------------

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return false;
        }
        switch (v.getKind()) {
            case BOOL:
                return v.asBoolean();
            case LONG:
                return v.asLong() != 0L;
            case DOUBLE:
                return v.asDouble() != 0d;
            case STRING:
                return Boolean.parseBoolean(v.asString().trim()); // 规范语义：非 "true"(ci) → false
            default:
                throw Errors.conversion("cannot convert " + v.getKind() + " value to boolean");
        }
    }

    // ------------------------------------------------------------------
    // 字符串
    // ------------------------------------------------------------------

    @Override
    public String getString(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return null;
        }
        switch (v.getKind()) {
            case LONG:
                return String.valueOf(v.asLong());
            case DOUBLE:
                return Double.toString(v.asDouble());
            case BOOL:
                return String.valueOf(v.asBoolean());
            case STRING:
                return v.asString();
            case BYTES:
                return toHexString(v.asBytes());
            default:
                throw Errors.conversion("cannot convert " + v.getKind() + " value to String");
        }
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    // ------------------------------------------------------------------
    // 日期时间族（字符串形态见 §5.4/§六：JSON 通道经内核显示串）
    // ------------------------------------------------------------------

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return null;
        }
        // Date.valueOf 在默认时区下构造——QSDB DATE 无时区语义，规范姿势
        return Date.valueOf(parseLocalDate(v, colType(columnIndex).type));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return null;
        }
        return Time.valueOf(parseLocalTime(v));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return null;
        }
        return parseTimestamp(v);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // QSDB DATE 无时区语义；忽略 cal（文档化的宽容实现）
        return getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    private LocalDate parseLocalDate(Value v, cn.qinwh.qsdb.jdbc.types.QsdbType declared) throws SQLException {
        try {
            switch (v.getKind()) {
                case STRING:
                    return LocalDate.parse(v.asString().trim());
                case LONG:
                    if (declared == cn.qinwh.qsdb.jdbc.types.QsdbType.DATE) {
                        return LocalDate.ofEpochDay(v.asLong());
                    }
                    throw Errors.conversion("cannot interpret numeric value as DATE");
                default:
                    throw Errors.conversion("cannot convert " + v.getKind() + " value to DATE");
            }
        } catch (java.time.format.DateTimeParseException e) {
            throw Errors.conversion("cannot parse '" + v + "' as DATE (expected yyyy-MM-dd): "
                    + e.getMessage());
        }
    }

    private LocalTime parseLocalTime(Value v) throws SQLException {
        if (v.getKind() == Value.Kind.LONG) {
            // rows 二进制通道：自午夜纳秒
            return LocalTime.ofNanoOfDay(v.asLong());
        }
        try {
            if (v.getKind() == Value.Kind.STRING) {
                return LocalTime.parse(v.asString().trim());
            }
        } catch (java.time.format.DateTimeParseException e) {
            throw Errors.conversion("cannot parse '" + v + "' as TIME (expected HH:MM:SS[.f…]): "
                    + e.getMessage());
        }
        throw Errors.conversion("cannot convert " + v.getKind() + " value to TIME");
    }

    private Timestamp parseTimestamp(Value v) throws SQLException {
        if (v.getKind() == Value.Kind.LONG) {
            // rows 二进制通道（naive TIMESTAMP）：自 epoch 纳秒，语义是"该纳秒值
            // 即 UTC 墙钟"——与 JSON 通道把 naive 显示串按本地墙钟解析的结果一致
            long nanos = v.asLong();
            LocalDateTime wall = LocalDateTime.ofEpochSecond(
                    Math.floorDiv(nanos, 1_000_000_000L),
                    (int) Math.floorMod(nanos, 1_000_000_000L), ZoneOffset.UTC);
            return Timestamp.valueOf(wall);
        }
        String s;
        switch (v.getKind()) {
            case STRING:
                s = v.asString().trim();
                break;
            default:
                throw Errors.conversion("cannot convert " + v.getKind() + " value to TIMESTAMP");
        }
        try {
            // 带时区形态（RFC3339：…Z / ±hh:mm）→ OffsetDateTime
            if (s.endsWith("Z") || s.endsWith("z")) {
                return Timestamp.from(OffsetDateTime.parse(s).toInstant());
            }
            int tPos = s.indexOf('T');
            if (tPos >= 11 && (s.indexOf('+', tPos) > 0
                    || (s.indexOf('-', tPos) > 0 && s.indexOf('-', tPos) > tPos + 8))) {
                return Timestamp.from(OffsetDateTime.parse(s).toInstant());
            }
            // naive 形态：内核显示串是 'T' 分隔，JDBC valueOf 要空格分隔
            String spaced = tPos >= 0 ? s.substring(0, tPos) + ' ' + s.substring(tPos + 1) : s;
            return Timestamp.valueOf(spaced);
        } catch (java.time.format.DateTimeParseException
                | IllegalArgumentException e) {
            throw Errors.conversion("cannot parse '" + s + "' as TIMESTAMP"
                    + " (expected yyyy-MM-dd[ T]HH:MM:SS[.f…] or RFC3339): " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // 二进制
    // ------------------------------------------------------------------

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return null;
        }
        switch (v.getKind()) {
            case BYTES:
                return v.asBytes();
            case STRING:
                return fromHexString(v.asString());
            case LONG:
                // 已知边界（驱动反馈 J2，旧 server 数字启发）：偶数位 hex 会被吞成整数，
                // 前导零已丢——尽力还原并文档化
                return fromHexString(padEvenHex(Long.toHexString(v.asLong())));
            default:
                throw Errors.conversion("cannot convert " + v.getKind() + " value to byte[]");
        }
    }

    private static String padEvenHex(String hex) {
        return hex.length() % 2 == 0 ? hex : "0" + hex;
    }

    private static byte[] fromHexString(String s) throws SQLException {
        String hex = s.trim();
        if (hex.length() % 2 != 0) {
            throw Errors.conversion("binary hex string has odd length " + hex.length()
                    + " (leading zero likely lost in old-server JSON numeric heuristic;"
                    + " upgrade the server or read BINARY via the rows channel): '" + s + "'");
        }
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            try {
                out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            } catch (NumberFormatException e) {
                throw Errors.conversion("invalid hex byte at index " + (i * 2) + " in '" + s + "'");
            }
        }
        return out;
    }

    private static String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // getObject 族（§6.1 规范映射，类型驱动）
    // ------------------------------------------------------------------

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Value v = cell(columnIndex);
        if (v.isNull()) {
            return null;
        }
        cn.qinwh.qsdb.jdbc.types.QsdbType t = colType(columnIndex).type;
        switch (t) {
            case TINYINT:
                long b = getLong(columnIndex);
                if (b < Byte.MIN_VALUE || b > Byte.MAX_VALUE) {
                    throw Errors.numericOutOfRange("value " + b + " does not fit in BYTE");
                }
                return (byte) b;
            case SMALLINT:
            case INT:
                int i = getInt(columnIndex);
                return i;
            case BIGINT:
                return getLong(columnIndex);
            case FLOAT:
                return getFloat(columnIndex);
            case DOUBLE:
                return getDouble(columnIndex);
            case DECIMAL:
                return getBigDecimal(columnIndex);
            case BOOLEAN:
                return getBoolean(columnIndex);
            case VARCHAR:
            case TEXT:
            case INTERVAL:
                return getString(columnIndex);
            case DATE:
                return getDate(columnIndex);
            case TIME:
                return getTime(columnIndex);
            case TIMESTAMP:
                return getTimestamp(columnIndex);
            case BINARY:
                return getBytes(columnIndex);
            default:
                throw Errors.conversion("unsupported column type for getObject: " + t);
        }
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        if (map != null && !map.isEmpty()) {
            throw Errors.unsupported("custom type maps are not supported");
        }
        return getObject(columnIndex);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object o = getObject(columnIndex);
        if (o == null) {
            return null;
        }
        if (type.isInstance(o)) {
            return type.cast(o);
        }
        throw Errors.conversion("value of " + o.getClass().getName() + " cannot be returned as "
                + type.getName());
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    // ------------------------------------------------------------------
    // 流族（大字段便捷读取）
    // ------------------------------------------------------------------

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        byte[] b = getBytes(columnIndex);
        return b == null ? null : new ByteArrayInputStream(b);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        return s == null ? null : new ByteArrayInputStream(s.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    }

    @Override
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        return s == null ? null : new ByteArrayInputStream(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        return s == null ? null : new StringReader(s);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public java.net.URL getURL(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s == null) {
            return null;
        }
        try {
            return new URI(s).toURL();
        } catch (Exception e) {
            throw Errors.conversion("cannot interpret '" + s + "' as URL: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // 按列名取值（统一委托按索引）
    // ------------------------------------------------------------------

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public java.net.URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    // ------------------------------------------------------------------
    // 元数据/属性
    // ------------------------------------------------------------------

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureOpen();
        return QsdbResultSetMetaData.of(columns);
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public int getType() {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return CONCUR_READ_ONLY;
    }

    @Override
    public int getHoldability() {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getFetchSize() {
        return 0; // JSON 一次性返回，无 fetch 概念（§十一）
    }

    @Override
    public void setFetchSize(int rows) {
        // no-op（Statement 侧同样记录 debug 日志）
    }

    @Override
    public int getFetchDirection() {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw Errors.unsupported("only FETCH_FORWARD is supported (forward-only result set)");
        }
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public String getCursorName() throws SQLException {
        throw Errors.unsupported("positioned updates/cursors are not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw Errors.invalid("does not implement '" + iface.getName() + "'");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    // ------------------------------------------------------------------
    // 不支持：游标移动 / 更新 / 大对象（v1 显式不做，§十五）
    // ------------------------------------------------------------------

    private SQLException forwardOnly(String method) {
        return Errors.invalid(method + " is not supported on a TYPE_FORWARD_ONLY result set");
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        ensureOpen();
        return position == -1 && !rows.isEmpty();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        ensureOpen();
        if (rows.isEmpty()) {
            return false;
        }
        long limit = maxRows > 0 ? Math.min((long) rows.size(), maxRows) : rows.size();
        return position >= limit;
    }

    @Override
    public boolean isFirst() throws SQLException {
        ensureOpen();
        return position == 0 && !rows.isEmpty();
    }

    @Override
    public boolean isLast() throws SQLException {
        ensureOpen();
        long limit = maxRows > 0 ? Math.min((long) rows.size(), maxRows) : rows.size();
        return position == limit - 1;
    }

    @Override
    public boolean first() throws SQLException {
        throw forwardOnly("first()");
    }

    @Override
    public boolean last() throws SQLException {
        throw forwardOnly("last()");
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw forwardOnly("absolute()");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw forwardOnly("relative()");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw forwardOnly("beforeFirst()");
    }

    @Override
    public void afterLast() throws SQLException {
        throw forwardOnly("afterLast()");
    }

    @Override
    public boolean previous() throws SQLException {
        throw forwardOnly("previous()");
    }

    @Override
    public void insertRow() throws SQLException {
        throw Errors.unsupported("result set updates are not supported (v1 read-only)");
    }

    @Override
    public void updateRow() throws SQLException {
        throw Errors.unsupported("result set updates are not supported (v1 read-only)");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw Errors.unsupported("result set updates are not supported (v1 read-only)");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw Errors.unsupported("result set refresh is not supported");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw Errors.unsupported("result set updates are not supported (v1 read-only)");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw Errors.unsupported("result set updates are not supported (v1 read-only)");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw Errors.unsupported("result set updates are not supported (v1 read-only)");
    }

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    private SQLException updateUnsupported(String method) {
        return Errors.unsupported("ResultSet." + method + " is not supported (v1 read-only)");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw updateUnsupported("updateNull");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw updateUnsupported("updateBoolean");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw updateUnsupported("updateByte");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw updateUnsupported("updateShort");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw updateUnsupported("updateInt");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw updateUnsupported("updateLong");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw updateUnsupported("updateFloat");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw updateUnsupported("updateDouble");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw updateUnsupported("updateBigDecimal");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw updateUnsupported("updateString");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw updateUnsupported("updateBytes");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw updateUnsupported("updateDate");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw updateUnsupported("updateTime");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw updateUnsupported("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw updateUnsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw updateUnsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw updateUnsupported("updateCharacterStream");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw updateUnsupported("updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw updateUnsupported("updateObject");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw updateUnsupported("updateNull");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw updateUnsupported("updateBoolean");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw updateUnsupported("updateByte");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw updateUnsupported("updateShort");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw updateUnsupported("updateInt");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw updateUnsupported("updateLong");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw updateUnsupported("updateFloat");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw updateUnsupported("updateDouble");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw updateUnsupported("updateBigDecimal");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw updateUnsupported("updateString");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw updateUnsupported("updateBytes");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw updateUnsupported("updateDate");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw updateUnsupported("updateTime");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw updateUnsupported("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw updateUnsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw updateUnsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException {
        throw updateUnsupported("updateCharacterStream");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw updateUnsupported("updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw updateUnsupported("updateObject");
    }

    @Override
    public void updateRowId(int columnIndex, java.sql.RowId x) throws SQLException {
        throw updateUnsupported("updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, java.sql.RowId x) throws SQLException {
        throw updateUnsupported("updateRowId");
    }

    @Override
    public java.sql.RowId getRowId(int columnIndex) throws SQLException {
        throw Errors.unsupported("RowId is not supported (no row-level identity)");
    }

    @Override
    public java.sql.RowId getRowId(String columnLabel) throws SQLException {
        throw Errors.unsupported("RowId is not supported (no row-level identity)");
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw updateUnsupported("updateNString");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw updateUnsupported("updateNString");
    }

    @Override
    public void updateNClob(int columnIndex, java.sql.NClob nClob) throws SQLException {
        throw updateUnsupported("updateNClob");
    }

    @Override
    public void updateNClob(int columnIndex, java.io.Reader reader) throws SQLException {
        throw updateUnsupported("updateNClob");
    }

    @Override
    public void updateNClob(int columnIndex, java.io.Reader reader, long length) throws SQLException {
        throw updateUnsupported("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, java.sql.NClob nClob) throws SQLException {
        throw updateUnsupported("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, java.io.Reader reader) throws SQLException {
        throw updateUnsupported("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        throw updateUnsupported("updateNClob");
    }

    @Override
    public void updateSQLXML(int columnIndex, java.sql.SQLXML xmlObject) throws SQLException {
        throw updateUnsupported("updateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel, java.sql.SQLXML xmlObject) throws SQLException {
        throw updateUnsupported("updateSQLXML");
    }

    @Override
    public void updateBlob(int columnIndex, java.sql.Blob x) throws SQLException {
        throw updateUnsupported("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, java.sql.Blob x) throws SQLException {
        throw updateUnsupported("updateBlob");
    }

    @Override
    public void updateBlob(int columnIndex, java.io.InputStream inputStream) throws SQLException {
        throw updateUnsupported("updateBlob");
    }

    @Override
    public void updateBlob(int columnIndex, java.io.InputStream inputStream, long length)
            throws SQLException {
        throw updateUnsupported("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream) throws SQLException {
        throw updateUnsupported("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream, long length)
            throws SQLException {
        throw updateUnsupported("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, java.sql.Clob x) throws SQLException {
        throw updateUnsupported("updateClob");
    }

    @Override
    public void updateClob(int columnIndex, java.io.Reader reader) throws SQLException {
        throw updateUnsupported("updateClob");
    }

    @Override
    public void updateClob(int columnIndex, java.io.Reader reader, long length) throws SQLException {
        throw updateUnsupported("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, java.sql.Clob x) throws SQLException {
        throw updateUnsupported("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, java.io.Reader reader) throws SQLException {
        throw updateUnsupported("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        throw updateUnsupported("updateClob");
    }

    @Override
    public void updateArray(int columnIndex, java.sql.Array x) throws SQLException {
        throw updateUnsupported("updateArray");
    }

    @Override
    public void updateArray(String columnLabel, java.sql.Array x) throws SQLException {
        throw updateUnsupported("updateArray");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw updateUnsupported("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw updateUnsupported("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw updateUnsupported("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw updateUnsupported("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw updateUnsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw updateUnsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw updateUnsupported("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw updateUnsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw updateUnsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw updateUnsupported("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw updateUnsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw updateUnsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw updateUnsupported("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw updateUnsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw updateUnsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader x) throws SQLException {
        throw updateUnsupported("updateCharacterStream");
    }

    @Override
    public void updateObject(int columnIndex, Object x, java.sql.SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        throw updateUnsupported("updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x, java.sql.SQLType targetSqlType) throws SQLException {
        throw updateUnsupported("updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x, java.sql.SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        throw updateUnsupported("updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x, java.sql.SQLType targetSqlType) throws SQLException {
        throw updateUnsupported("updateObject");
    }

    @Override
    public void updateRef(int columnIndex, java.sql.Ref x) throws SQLException {
        throw updateUnsupported("updateRef");
    }

    @Override
    public void updateRef(String columnLabel, java.sql.Ref x) throws SQLException {
        throw updateUnsupported("updateRef");
    }

    @Override
    public java.sql.Ref getRef(int columnIndex) throws SQLException {
        throw Errors.unsupported("Ref is not supported");
    }

    @Override
    public java.sql.Ref getRef(String columnLabel) throws SQLException {
        throw Errors.unsupported("Ref is not supported");
    }

    @Override
    public java.sql.Array getArray(int columnIndex) throws SQLException {
        throw Errors.unsupported("Array columns are not supported (v1 read-only)");
    }

    @Override
    public java.sql.Array getArray(String columnLabel) throws SQLException {
        throw Errors.unsupported("Array columns are not supported (v1 read-only)");
    }

    @Override
    public java.sql.SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw Errors.unsupported("SQLXML is not supported");
    }

    @Override
    public java.sql.SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw Errors.unsupported("SQLXML is not supported");
    }

    @Override
    public java.sql.Blob getBlob(int columnIndex) throws SQLException {
        throw Errors.unsupported("LOB locators are not supported; use getBytes");
    }

    @Override
    public java.sql.Blob getBlob(String columnLabel) throws SQLException {
        throw Errors.unsupported("LOB locators are not supported; use getBytes");
    }

    @Override
    public java.sql.Clob getClob(int columnIndex) throws SQLException {
        throw Errors.unsupported("LOB locators are not supported; use getString/getCharacterStream");
    }

    @Override
    public java.sql.Clob getClob(String columnLabel) throws SQLException {
        throw Errors.unsupported("LOB locators are not supported; use getString/getCharacterStream");
    }

    @Override
    public java.sql.NClob getNClob(int columnIndex) throws SQLException {
        throw Errors.unsupported("LOB locators are not supported; use getNString");
    }

    @Override
    public java.sql.NClob getNClob(String columnLabel) throws SQLException {
        throw Errors.unsupported("LOB locators are not supported; use getNString");
    }

    // ------------------------------------------------------------------
    // 数值解析辅助
    // ------------------------------------------------------------------

    private static long parseLong(String s) throws SQLException {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            try {
                double d = Double.parseDouble(s);
                if (Double.isNaN(d) || d >= 9.223372036854776E18 || d <= -9.223372036854776E18) {
                    throw Errors.numericOutOfRange("value '" + s + "' does not fit in BIGINT");
                }
                return (long) d;
            } catch (NumberFormatException e2) {
                throw Errors.conversion("cannot convert '" + s + "' to long");
            }
        }
    }

    private static double parseDouble(String s) throws SQLException {
        // 内核对非有限浮点输出 Rust Display 形态（"NaN"/"inf"/"-inf"）——顺带兼容 Java 拼写
        String norm = s;
        if ("nan".equalsIgnoreCase(s)) {
            norm = "NaN";
        } else if ("inf".equalsIgnoreCase(s) || "infinity".equalsIgnoreCase(s)) {
            norm = "Infinity";
        } else if ("-inf".equalsIgnoreCase(s) || "-infinity".equalsIgnoreCase(s)) {
            norm = "-Infinity";
        }
        try {
            return Double.parseDouble(norm);
        } catch (NumberFormatException e) {
            throw Errors.conversion("cannot convert '" + s + "' to double");
        }
    }
}
