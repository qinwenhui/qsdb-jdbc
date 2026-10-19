package cn.qinwh.qsdb.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.qinwh.qsdb.jdbc.internal.Errors;
import cn.qinwh.qsdb.jdbc.internal.RowsEncoder;
import cn.qinwh.qsdb.jdbc.internal.TypeStrings;
import cn.qinwh.qsdb.jdbc.transport.TableSchema;
import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * 批量写入器：走内核 {@code qsdb-rows} 行式零依赖通道
 * （{@code POST /api/v1/write?format=rows}），面向按行取值的客户端——
 * 零转置、可流式、内存恒定。
 *
 * <p><b>与 {@code executeBatch} 的分工</b>（行业惯例：pgjdbc 的 CopyManager、
 * SQL Server 的 SQLServerBulkCopy 都是"保留 batch + 另开批量 API"）：
 * <ul>
 *   <li>{@code executeBatch}——标准 JDBC 语义，走 SQL 通道（多行 VALUES 合并），
 *       适合中小批量；</li>
 *   <li><b>本类</b>——大批量装载专用，绕开 SQL 文本解析（内核实测 5M 行
 *       SQL 通道 ~44s vs 批量通道 ~2s 量级）。</li>
 * </ul>
 *
 * <p><b>用法</b>：
 * <pre>{@code
 * try (QsdbBulkWriter w = conn.bulkWriter("orders")) {
 *     for (Order o : orders) {
 *         w.setLong("id", o.id);
 *         w.setString("name", o.name);
 *         w.setDate("dt", o.dt);
 *         w.addRow();          // 攒够阈值自动编码 + 提交一个 chunk
 *     }
 * }   // close() 提交剩余并返回累计行数
 * }</pre>
 *
 * <p><b>语义</b>：{@code mode=append} 下**一次请求 = 一次提交**——驱动按
 * 分块阈值（默认 65536 行）自动分批，故内存恒定、与总数据量无关。
 * 失败时此前已提交的块已生效（内核无事务模型）。
 *
 * <p><b>线程模型</b>：按 JDBC 惯例非线程安全（每线程一个写入器）。
 * 并发装载用多个连接各自创建写入器（实测 4–8 并发为甜点区）。
 */
public final class QsdbBulkWriter implements AutoCloseable {

    /** 默认分块行数（与 SQL 通道的合并阈值同档）。 */
    public static final int DEFAULT_CHUNK_ROWS = 65536;

    private final QsdbConnection connection;
    private final String table;
    private final List<RowsEncoder.Column> columns;
    private final Map<String, Integer> indexByName = new HashMap<String, Integer>();
    private final int chunkRows;

    private Object[][] buffer;   // [行][列]，行主序（row-major，零转置）
    private int buffered;
    private long totalRows;
    private boolean closed;
    private boolean overwriteMode;        // true = 每次 flush 分区级替换（非追加）
    private Long expectedGeneration;      // 非 null = CAS 写入（F24）

    /**
     * 工厂：从任意 {@link java.sql.Connection} 引用创建（内部走
     * {@code unwrap}，故连接池代理亦可——避免 pgjdbc {@code (BaseConnection)}
     * 那种手工强转）。
     *
     * <pre>{@code
     * try (QsdbBulkWriter w = QsdbBulkWriter.open(conn, "orders")) { ... }
     * }</pre>
     */
    public static QsdbBulkWriter open(java.sql.Connection connection, String table) throws SQLException {
        return unwrap(connection).bulkWriter(table);
    }

    /** 同上，指定分块行数。 */
    public static QsdbBulkWriter open(java.sql.Connection connection, String table, int chunkRows)
            throws SQLException {
        return unwrap(connection).bulkWriter(table, chunkRows);
    }

    private static QsdbConnection unwrap(java.sql.Connection connection) throws SQLException {
        if (connection == null) {
            throw Errors.invalid("connection is null");
        }
        if (connection instanceof QsdbConnection) {
            return (QsdbConnection) connection;
        }
        try {
            return connection.unwrap(QsdbConnection.class);
        } catch (SQLException e) {
            throw Errors.invalid("connection is not a QSDB connection ("
                    + connection.getClass().getName() + "); the bulk channel is driver-specific");
        }
    }

    QsdbBulkWriter(QsdbConnection connection, String table, int chunkRows) throws SQLException {
        this.connection = connection;
        this.table = table;
        this.chunkRows = chunkRows > 0 ? chunkRows : DEFAULT_CHUNK_ROWS;
        TableSchema schema = connection.cachedTableSchema(table); // 表列序（含分区列）
        if (schema.getColumns().isEmpty()) {
            throw Errors.invalid("table '" + table + "' has no columns (does it exist?)");
        }
        List<RowsEncoder.Column> cols = new ArrayList<RowsEncoder.Column>(schema.getColumns().size());
        for (TableSchema.Column c : schema.getColumns()) {
            TypeStrings.Parsed p = TypeStrings.parse(c.type); // LogicalType 方言
            cols.add(new RowsEncoder.Column(c.name, p.type, p.scale));
            indexByName.put(c.name.toLowerCase(java.util.Locale.ROOT), cols.size() - 1);
        }
        this.columns = cols;
        this.buffer = new Object[this.chunkRows][];
    }

    // ------------------------------------------------------------------
    // 取值定位
    // ------------------------------------------------------------------

    /** 列数（= 表列数，含分区列）。 */
    public int getColumnCount() {
        return columns.size();
    }

    private int indexOf(String columnName) throws SQLException {
        Integer i = indexByName.get(columnName.toLowerCase(java.util.Locale.ROOT));
        if (i == null) {
            throw Errors.invalid("column not found in table '" + table + "': '" + columnName + "'");
        }
        return i;
    }

    private void checkIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columns.size()) {
            throw Errors.invalid("column index " + columnIndex + " out of range 1.." + columns.size());
        }
    }

    private QsdbType typeOf(int idx) {
        return columns.get(idx).type;
    }

    // ------------------------------------------------------------------
    // setXxx（按列序号；列名重载见下）
    // ------------------------------------------------------------------

    private void put(int idx, Object canonical) throws SQLException {
        ensureOpen();
        if (buffered >= chunkRows) {
            throw Errors.invalid("row buffer is full — call addRow() to submit it");
        }
        Object[] row = buffer[buffered];
        if (row == null) {
            row = new Object[columns.size()];
            buffer[buffered] = row;
        }
        row[idx] = canonical;
    }

    private void put(int idx, long v) throws SQLException {
        QsdbType t = typeOf(idx);
        switch (t) {
            case BIGINT:
                put(idx, Long.valueOf(v));
                return;
            case INT:
                if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                    throw Errors.numericOutOfRange("value " + v + " does not fit column INT");
                }
                put(idx, Long.valueOf(v));
                return;
            case SMALLINT:
                if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                    throw Errors.numericOutOfRange("value " + v + " does not fit column SMALLINT");
                }
                put(idx, Long.valueOf(v));
                return;
            case TINYINT:
                if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                    throw Errors.numericOutOfRange("value " + v + " does not fit column TINYINT");
                }
                put(idx, Long.valueOf(v));
                return;
            default:
                throw Errors.invalid("column " + columns.get(idx).name + " is " + t
                        + ", cannot set an integer value");
        }
    }

    private void putFloating(int idx, double v) throws SQLException {
        QsdbType t = typeOf(idx);
        if (t == QsdbType.DOUBLE || t == QsdbType.FLOAT) {
            put(idx, Double.valueOf(v)); // FLOAT 在编码期窄化为 f32
            return;
        }
        throw Errors.invalid("column " + columns.get(idx).name + " is " + t
                + ", cannot set a floating-point value");
    }

    /** 按列序号设置 64 位整数（须为整数列）。 */
    public void setLong(int columnIndex, long v) throws SQLException {
        checkIndex(columnIndex);
        put(columnIndex - 1, v);
    }

    /** 按列序号设置 32 位整数。 */
    public void setInt(int columnIndex, int v) throws SQLException {
        checkIndex(columnIndex);
        put(columnIndex - 1, (long) v);
    }

    /** 按列序号设置双精度浮点。 */
    public void setDouble(int columnIndex, double v) throws SQLException {
        checkIndex(columnIndex);
        putFloating(columnIndex - 1, v);
    }

    /** 按列序号设置布尔。 */
    public void setBoolean(int columnIndex, boolean v) throws SQLException {
        checkIndex(columnIndex);
        int idx = columnIndex - 1;
        if (typeOf(idx) != QsdbType.BOOLEAN) {
            throw Errors.invalid("column " + columns.get(idx).name + " is " + typeOf(idx)
                    + ", cannot set a boolean value");
        }
        put(idx, Boolean.valueOf(v));
    }

    /** 按列序号设置精确十进制（标度不符**显式拒绝**，不静默四舍五入）。 */
    public void setBigDecimal(int columnIndex, BigDecimal v) throws SQLException {
        checkIndex(columnIndex);
        int idx = columnIndex - 1;
        if (typeOf(idx) != QsdbType.DECIMAL) {
            throw Errors.invalid("column " + columns.get(idx).name + " is " + typeOf(idx)
                    + ", cannot set a decimal value");
        }
        put(idx, v);
    }

    /** 按列序号设置字符串（须为字符列）。 */
    public void setString(int columnIndex, String v) throws SQLException {
        checkIndex(columnIndex);
        int idx = columnIndex - 1;
        QsdbType t = typeOf(idx);
        if (t != QsdbType.VARCHAR && t != QsdbType.TEXT) {
            throw Errors.invalid("column " + columns.get(idx).name + " is " + t
                    + ", cannot set a string value");
        }
        put(idx, v);
    }

    /** 按列序号设置字节串（须为 BINARY 列）。 */
    public void setBytes(int columnIndex, byte[] v) throws SQLException {
        checkIndex(columnIndex);
        int idx = columnIndex - 1;
        if (typeOf(idx) != QsdbType.BINARY) {
            throw Errors.invalid("column " + columns.get(idx).name + " is " + typeOf(idx)
                    + ", cannot set a binary value");
        }
        put(idx, v);
    }

    /** 按列序号设置日期。 */
    public void setDate(int columnIndex, Date v) throws SQLException {
        setDate(columnIndex, v == null ? null : v.toLocalDate());
    }

    /** 按列序号设置日期（java.time）。 */
    public void setDate(int columnIndex, LocalDate v) throws SQLException {
        checkIndex(columnIndex);
        int idx = columnIndex - 1;
        if (typeOf(idx) != QsdbType.DATE) {
            throw Errors.invalid("column " + columns.get(idx).name + " is " + typeOf(idx)
                    + ", cannot set a date value");
        }
        put(idx, v == null ? null : Long.valueOf(v.toEpochDay()));
    }

    /** 按列序号设置时间。 */
    public void setTime(int columnIndex, Time v) throws SQLException {
        setTime(columnIndex, v == null ? null : v.toLocalTime());
    }

    /** 按列序号设置时间（java.time）。 */
    public void setTime(int columnIndex, LocalTime v) throws SQLException {
        checkIndex(columnIndex);
        int idx = columnIndex - 1;
        if (typeOf(idx) != QsdbType.TIME) {
            throw Errors.invalid("column " + columns.get(idx).name + " is " + typeOf(idx)
                    + ", cannot set a time value");
        }
        put(idx, v == null ? null : Long.valueOf(v.toNanoOfDay()));
    }

    /** 按列序号设置时间戳（naive UTC 语义，与内核 TIMESTAMP 一致）。 */
    public void setTimestamp(int columnIndex, Timestamp v) throws SQLException {
        setTimestamp(columnIndex, v == null ? null : v.toLocalDateTime());
    }

    /** 按列序号设置时间戳（java.time；按 UTC 折算为自 epoch 纳秒）。 */
    public void setTimestamp(int columnIndex, LocalDateTime v) throws SQLException {
        checkIndex(columnIndex);
        int idx = columnIndex - 1;
        if (typeOf(idx) != QsdbType.TIMESTAMP) {
            throw Errors.invalid("column " + columns.get(idx).name + " is " + typeOf(idx)
                    + ", cannot set a timestamp value");
        }
        // 精确换算：秒×1e9 + 纳秒（不用 toEpochMilli，避免负 epoch 截断出错）
        put(idx, v == null ? null : Long.valueOf(
                Math.addExact(Math.multiplyExact(v.toEpochSecond(ZoneOffset.UTC), 1_000_000_000L),
                        v.getNano())));
    }

    /** 按列序号设置 NULL（任意列类型均可）。 */
    public void setNull(int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        put(columnIndex - 1, null);
    }

    // ---- 按列名重载（等价委托）----

    /** 按列名设置 64 位整数。 */
    public void setLong(String columnName, long v) throws SQLException {
        setLong(indexOf(columnName) + 1, v);
    }

    /** 按列名设置 32 位整数。 */
    public void setInt(String columnName, int v) throws SQLException {
        setInt(indexOf(columnName) + 1, v);
    }

    /** 按列名设置双精度浮点。 */
    public void setDouble(String columnName, double v) throws SQLException {
        setDouble(indexOf(columnName) + 1, v);
    }

    /** 按列名设置布尔。 */
    public void setBoolean(String columnName, boolean v) throws SQLException {
        setBoolean(indexOf(columnName) + 1, v);
    }

    /** 按列名设置十进制。 */
    public void setBigDecimal(String columnName, BigDecimal v) throws SQLException {
        setBigDecimal(indexOf(columnName) + 1, v);
    }

    /** 按列名设置字符串。 */
    public void setString(String columnName, String v) throws SQLException {
        setString(indexOf(columnName) + 1, v);
    }

    /** 按列名设置字节串。 */
    public void setBytes(String columnName, byte[] v) throws SQLException {
        setBytes(indexOf(columnName) + 1, v);
    }

    /** 按列名设置日期。 */
    public void setDate(String columnName, LocalDate v) throws SQLException {
        setDate(indexOf(columnName) + 1, v);
    }

    /** 按列名设置日期（java.sql.Date）。 */
    public void setDate(String columnName, Date v) throws SQLException {
        setDate(indexOf(columnName) + 1, v);
    }

    /** 按列名设置时间。 */
    public void setTime(String columnName, LocalTime v) throws SQLException {
        setTime(indexOf(columnName) + 1, v);
    }

    /** 按列名设置时间（java.sql.Time）。 */
    public void setTime(String columnName, Time v) throws SQLException {
        setTime(indexOf(columnName) + 1, v);
    }

    /** 按列名设置时间戳。 */
    public void setTimestamp(String columnName, LocalDateTime v) throws SQLException {
        setTimestamp(indexOf(columnName) + 1, v);
    }

    /** 按列名设置时间戳（java.sql.Timestamp）。 */
    public void setTimestamp(String columnName, Timestamp v) throws SQLException {
        setTimestamp(indexOf(columnName) + 1, v);
    }

    /** 按列名设置 NULL。 */
    public void setNull(String columnName) throws SQLException {
        setNull(indexOf(columnName) + 1);
    }

    // ------------------------------------------------------------------
    // 行与提交
    // ------------------------------------------------------------------

    /**
     * 提交当前行（未设置的列视为 NULL），攒够分块阈值时自动发送一块。
     */
    public void addRow() throws SQLException {
        ensureOpen();
        if (buffered >= chunkRows) {
            throw Errors.invalid("row buffer is full");
        }
        // 行数组按需惰性创建：未调用 setXxx 的列天然为 NULL（格式要求 NULL
        // 也要占位，编码期统一处理）
        if (buffer[buffered] == null) {
            buffer[buffered] = new Object[columns.size()];
        }
        buffered++;
        if (buffered >= chunkRows) {
            flush();
        }
    }

    /**
     * 提交缓冲区中的行（编码为一个 chunk 并 POST）。空闲时为空操作。
     *
     * @return 本次提交的行数
     */
    public long flush() throws SQLException {
        ensureOpen();
        if (buffered == 0) {
            return 0;
        }
        int n = buffered;
        List<Object[]> rows = new ArrayList<Object[]>(n);
        for (int i = 0; i < n; i++) {
            Object[] row = buffer[i];
            rows.add(row == null ? new Object[columns.size()] : row);
        }
        List<List<Object[]>> chunks = new ArrayList<List<Object[]>>(1);
        chunks.add(rows);
        byte[] payload = RowsEncoder.encode(columns, chunks);
        connection.transport().writeBulk(connection.queryDatabase(), table,
                overwriteMode ? "overwrite" : "append", expectedGeneration, payload);
        // 重置缓冲（行数组不复用——已随 payload 编码完成，交给 GC）
        for (int i = 0; i < n; i++) {
            buffer[i] = null;
        }
        buffered = 0;
        totalRows += n;
        return n;
    }

    /** 累计已提交行数。 */
    public long getRowCount() {
        return totalRows;
    }

    /**
     * 切到 <b>overwrite 模式</b>（P3）：此后每次 {@link #flush()} 走
     * {@code mode=overwrite}——触及的分区<b>整份替换</b>为本次写入内容
     * （幂等回写原语：同一批数据重放结果一致，与 append 的"重试即重复"
     * 相反）。**只对单分区写入有意义**——多分区写入会替换每个触及分区，
     * 误用即数据丢失；跨分块的多批 overwrite 也只有最后一批生效，务必
     * 一次 {@code flush()} 提交完整分区内容（chunkRows ≥ 分区行数）。
     */
    public QsdbBulkWriter overwrite() {
        this.overwriteMode = true;
        return this;
    }

    /**
     * 切到 <b>overwrite + CAS 模式</b>（内核 F24 乐观并发）：每次 flush 带
     * {@code expected_generation}——分区当前 generation 不匹配 →
     * {@code WRITE_CONFLICT}（SQLState 40001，**动作 = 重读最新 generation
     * 再决定**，不是盲目重试）。只支持单分区（一次请求命中多分区 → 400）。
     *
     * <p>期望值来源：{@link QsdbConnection#getTablePartitions(String)} 的
     * {@code PartitionInfo.generation}（分区提交计数器）。
     *
     * @param expectedGeneration 期望的分区当前 generation
     */
    public QsdbBulkWriter overwriteCas(long expectedGeneration) {
        this.overwriteMode = true;
        this.expectedGeneration = Long.valueOf(expectedGeneration);
        return this;
    }

    /** 提交剩余行并释放。 */
    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        try {
            flush();
        } finally {
            closed = true;
            buffer = null;
        }
    }

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw Errors.invalid("bulk writer is closed (table '" + table + "')");
        }
        if (connection.isClosed()) {
            throw Errors.invalid("connection is closed: " + connection);
        }
    }

    /** 脱敏展示。 */
    @Override
    public String toString() {
        return "QsdbBulkWriter(" + table + ", columns=" + columns.size()
                + ", chunkRows=" + chunkRows + ", rows=" + totalRows
                + (closed ? ", closed" : "") + ")";
    }
}
