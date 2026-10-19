package cn.qinwh.qsdb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import cn.qinwh.qsdb.jdbc.internal.Errors;
import cn.qinwh.qsdb.jdbc.internal.TypeStrings;
import cn.qinwh.qsdb.jdbc.transport.ColMeta;

/**
 * 查询结果列元数据（设计文档 §八）。
 *
 * <p>列名取响应 {@code schema[].name}（SELECT 别名经内核透出），
 * {@code getColumnLabel} 同值；类型经 TypeStrings 按 Arrow 方言解析。
 * 响应不带可空性——{@code isNullable} 如实报告
 * {@code columnNullableUnknown}。
 */
final class QsdbResultSetMetaData implements ResultSetMetaData {

    private final List<ColMeta> columns;
    private final TypeStrings.Parsed[] parsed; // 惰性填充

    QsdbResultSetMetaData(List<ColMeta> columns) {
        this.columns = columns;
        this.parsed = new TypeStrings.Parsed[columns.size()];
    }

    static QsdbResultSetMetaData of(List<ColMeta> columns) {
        return new QsdbResultSetMetaData(columns);
    }

    private TypeStrings.Parsed parsed(int column) throws SQLException {
        int idx = column - 1;
        if (idx < 0 || idx >= columns.size()) {
            throw Errors.invalid("column index " + column + " out of range 1.." + columns.size());
        }
        if (parsed[idx] == null) {
            parsed[idx] = TypeStrings.parse(columns.get(idx).getTypeName());
        }
        return parsed[idx];
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return name(column);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return name(column); // 内核透出的即最终列名（含别名）
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return parsed(column).type.getJdbcType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        // 保留内核原始形态（Arrow 方言）——诊断价值高于归一名
        int idx = column - 1;
        checkRange(idx);
        return columns.get(idx).getTypeName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        TypeStrings.Parsed p = parsed(column);
        return p.precision > 0 ? p.precision : p.type.getDefaultPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return parsed(column).scale;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return parsed(column).type.getObjectClass().getName();
    }

    @Override
    public int isNullable(int column) {
        return columnNullableUnknown; // §八：响应不带可空性
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return parsed(column).type.isNumeric();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        cn.qinwh.qsdb.jdbc.types.QsdbType t = parsed(column).type;
        return t.isCharacter() || t == cn.qinwh.qsdb.jdbc.types.QsdbType.BINARY;
    }

    @Override
    public boolean isSearchable(int column) {
        return true;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public boolean isReadOnly(int column) {
        return true;
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false; // 表格式存储无自增
    }

    @Override
    public String getSchemaName(int column) {
        return ""; // 不适用返回空串
    }

    @Override
    public String getTableName(int column) {
        return ""; // SELECT 投影后列与表不一一对应，不给猜测值
    }

    @Override
    public String getCatalogName(int column) {
        return "";
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        TypeStrings.Parsed p = parsed(column);
        switch (p.type) {
            case TINYINT:
                return 4;
            case SMALLINT:
                return 6;
            case INT:
                return 11;
            case BIGINT:
                return 20;
            case FLOAT:
                return 15;
            case DOUBLE:
                return 24;
            case DECIMAL:
                return Math.max(p.precision + 2, 8);
            case BOOLEAN:
                return 5;
            case DATE:
                return 10;
            case TIME:
                return 18;
            case TIMESTAMP:
                return 32;
            case BINARY:
                return Math.max(p.precision * 2, 4);
            case INTERVAL:
                return 40;
            default: // VARCHAR / TEXT
                int prec = p.precision > 0 ? p.precision : 65535;
                return Math.min(prec, 4096); // 显示用途，超长列截断提示值
        }
    }

    private String name(int column) throws SQLException {
        int idx = column - 1;
        checkRange(idx);
        String n = columns.get(idx).getName();
        return n == null ? "" : n;
    }

    private void checkRange(int idx) throws SQLException {
        if (idx < 0 || idx >= columns.size()) {
            throw Errors.invalid("column index " + (idx + 1) + " out of range 1.." + columns.size());
        }
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
}
