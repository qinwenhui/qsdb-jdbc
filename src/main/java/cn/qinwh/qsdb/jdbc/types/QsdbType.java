package cn.qinwh.qsdb.jdbc.types;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

/**
 * QSDB 逻辑类型到 JDBC 类型的一等映射（设计文档 §六）。
 *
 * <p>本枚举是驱动类型系统的中枢：
 * <ul>
 *   <li>读路径：{@code QsdbResultSet} 按列的 {@code QsdbType} 决定 {@code getXxx}
 *       转换与 {@code getObject} 返回的 Java 类型（类型驱动，而非值驱动——
 *       内核 JSON 通道有数字启发，字符串列的数值内容会以 JSON 数字到达）；</li>
 *   <li>写路径：{@code PreparedStatement} 的 {@code setXxx} 按渲染目标类型生成字面量；</li>
 *   <li>元数据：{@code ResultSetMetaData} / {@code DatabaseMetaData#getTypeInfo}
 *       报告的 JDBC 类型、精度、类名均取自本枚举。</li>
 * </ul>
 *
 * <p>两种 QSDB 类型字符串方言（Arrow {@code DataType::to_string()} 与内核
 * {@code LogicalType} Display）由 internal 包的 TypeStrings 统一解析到本枚举。
 *
 * <p>实例不可变且全枚举，天然线程安全。
 */
public enum QsdbType {

    /** 8 位有符号整数（内核 TINYINT / Arrow Int8）。 */
    TINYINT(Types.TINYINT, "TINYINT", 3, Byte.class),

    /** 16 位有符号整数（内核 SMALLINT / Arrow Int16）。 */
    SMALLINT(Types.SMALLINT, "SMALLINT", 5, Integer.class),

    /** 32 位有符号整数（内核 INT / Arrow Int32）。 */
    INT(Types.INTEGER, "INTEGER", 10, Integer.class),

    /** 64 位有符号整数（内核 BIGINT / Arrow Int64）。 */
    BIGINT(Types.BIGINT, "BIGINT", 19, Long.class),

    /** 4 字节 IEEE 浮点（内核 FLOAT / Arrow Float32）。 */
    FLOAT(Types.REAL, "REAL", 7, Float.class),

    /** 8 字节 IEEE 浮点（内核 DOUBLE / Arrow Float64）。 */
    DOUBLE(Types.DOUBLE, "DOUBLE", 15, Double.class),

    /**
     * 精确十进制（内核 DECIMAL(p,s) / Arrow Decimal128）。
     * 精度/标度从类型串解析，{@link #getDefaultPrecision} 不适用（返回 0）。
     * JSON 通道上有效精度约 15~16 位十进制（内核经 f64 中转，设计文档 §5.4）。
     */
    DECIMAL(Types.DECIMAL, "DECIMAL", 0, java.math.BigDecimal.class),

    /** 变长字符串（内核 STRING/VARCHAR(n)/CHAR(n) / Arrow Utf8）。 */
    VARCHAR(Types.VARCHAR, "VARCHAR", 65535, String.class),

    /** 长文本（保留：内核当前不产出，超长字符串 overflow 落地后启用）。 */
    TEXT(Types.LONGVARCHAR, "LONGVARCHAR", Integer.MAX_VALUE, String.class),

    /** 布尔（内核 BOOLEAN / Arrow Boolean）。 */
    BOOLEAN(Types.BOOLEAN, "BOOLEAN", 1, Boolean.class),

    /** 日期（内核 DATE / Arrow Date32，1970-01-01 起的天数）。 */
    DATE(Types.DATE, "DATE", 10, java.sql.Date.class),

    /** 一天之内的时间（内核 TIME / Arrow Time64(Nanosecond)，自午夜纳秒）。 */
    TIME(Types.TIME, "TIME", 18, java.sql.Time.class),

    /** 时间戳（内核 TIMESTAMP / Arrow Timestamp(Nanosecond, tz?)，naive UTC）。 */
    TIMESTAMP(Types.TIMESTAMP, "TIMESTAMP", 29, java.sql.Timestamp.class),

    /** 字节串（内核 BINARY / Arrow Binary）。 */
    BINARY(Types.BINARY, "BINARY", 0, byte[].class),

    /**
     * 月日纳秒间隔（内核 INTERVAL / Arrow Interval(MonthDayNano)）。
     * JDBC 无标准对应类型——驱动降级为 VARCHAR（值取 Arrow 显示串），
     * 仅 {@code getString} / {@code getObject} 可用，其余转换显式报错。
     */
    INTERVAL(Types.VARCHAR, "INTERVAL", 0, String.class);

    private final int jdbcType;
    private final String jdbcTypeName;
    private final int defaultPrecision;
    private final Class<?> objectClass;

    QsdbType(int jdbcType, String jdbcTypeName, int defaultPrecision, Class<?> objectClass) {
        this.jdbcType = jdbcType;
        this.jdbcTypeName = jdbcTypeName;
        this.defaultPrecision = defaultPrecision;
        this.objectClass = objectClass;
    }

    /** 对应的 {@code java.sql.Types} 常量。 */
    public int getJdbcType() {
        return jdbcType;
    }

    /** JDBC 类型名（{@code ResultSetMetaData#getColumnTypeName} 与 {@code getTypeInfo} 行名）。 */
    public String getJdbcTypeName() {
        return jdbcTypeName;
    }

    /**
     * 默认精度（元数据兜底值）。DECIMAL 从类型串解析精度，此值无意义；
     * VARCHAR 取内核上限 65535；BINARY 无界返回 0。
     */
    public int getDefaultPrecision() {
        return defaultPrecision;
    }

    /** {@code getObject} 规范返回的 Java 类（JDBC 附录 B 映射）。 */
    public Class<?> getObjectClass() {
        return objectClass;
    }

    /** 数值族（含 DECIMAL）。 */
    public boolean isNumeric() {
        return this == TINYINT || this == SMALLINT || this == INT || this == BIGINT
                || this == FLOAT || this == DOUBLE || this == DECIMAL;
    }

    /** 是否有符号整数族。 */
    public boolean isIntegral() {
        return this == TINYINT || this == SMALLINT || this == INT || this == BIGINT;
    }

    /** 字符串族（渲染字面量时需加引号转义）。 */
    public boolean isCharacter() {
        return this == VARCHAR || this == TEXT || this == INTERVAL;
    }

    /** 从 {@code java.sql.Types} 常量反查（参数路径 setNull/setObject 用）。 */
    public static QsdbType forJdbcType(int jdbcType) throws SQLException {
        for (QsdbType t : values()) {
            if (t.jdbcType == jdbcType) {
                return t;
            }
        }
        // BOOLEAN 在部分驱动侧等价 BIT——顺带收纳
        if (jdbcType == Types.BIT) {
            return BOOLEAN;
        }
        if (jdbcType == Types.CHAR || jdbcType == Types.NVARCHAR || jdbcType == Types.NCHAR
                || jdbcType == Types.LONGNVARCHAR || jdbcType == Types.CLOB) {
            return VARCHAR;
        }
        if (jdbcType == Types.NUMERIC) {
            return DECIMAL;
        }
        if (jdbcType == Types.REAL) {
            return FLOAT;
        }
        if (jdbcType == Types.LONGVARCHAR) {
            return TEXT;
        }
        throw new SQLFeatureNotSupportedException(
                "parameter JDBC type " + jdbcType + " is not supported by QSDB (v1 read-only)", "0A000");
    }
}
