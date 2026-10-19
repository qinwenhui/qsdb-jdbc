package cn.qinwh.qsdb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cn.qinwh.qsdb.jdbc.QsdbException;
import cn.qinwh.qsdb.jdbc.internal.Errors;
import cn.qinwh.qsdb.jdbc.internal.TypeStrings;
import cn.qinwh.qsdb.jdbc.transport.ColMeta;
import cn.qinwh.qsdb.jdbc.transport.DbInfo;
import cn.qinwh.qsdb.jdbc.transport.TableSchema;
import cn.qinwh.qsdb.jdbc.transport.Value;
import cn.qinwh.qsdb.jdbc.types.QsdbType;

/**
 * 库/表/列/类型元数据（设计文档 §八，工具兼容的关键层）。
 *
 * <p>原则：<b>能返回空集的不抛异常</b>（DBeaver/帆软探针失败即拒连），
 * 确不支持的才抛。表数据全部来自 {@code /api/v1/tables} 与
 * {@code /tables/{t}/schema}（经连接级缓存，§十一）；无对应元数据的
 * 方法（主键/索引/外键/存储过程等）返回规范列布局的空集——表格式存储
 * 本就没有这些概念，诚实呈现。
 *
 * <p>supports* 系列按内核已验证的 SQL 能力面如实回答（UNION/EXISTS/IN/
 * LIKE 已在内核 translate.rs 确认；事务/批量/存储过程等 v1 不支持）——
 * 工具按这些标志降级 UI，不吹牛。
 */
final class QsdbDatabaseMetaData implements DatabaseMetaData {

    private final QsdbConnection connection;
    private String cachedServerVersion;

    QsdbDatabaseMetaData(QsdbConnection connection) {
        this.connection = connection;
    }

    // ------------------------------------------------------------------
    // 通用工具
    // ------------------------------------------------------------------

    /** 构造内存结果集。 */
    private static ResultSet rows(List<ColMeta> cols, List<List<Value>> data) throws SQLException {
        return QsdbResultSet.forRows(cols, data);
    }

    private static ColMeta col(String name, QsdbType type) {
        // 元数据结果集的类型串统一用 LogicalType 方言（自产自销）
        return new ColMeta(name, type.getJdbcTypeName());
    }

    private static List<List<Value>> emptyRows() {
        return new ArrayList<List<Value>>();
    }

    private static List<Value> row(Object... cells) {
        List<Value> r = new ArrayList<Value>(cells.length);
        for (Object c : cells) {
            if (c == null) {
                r.add(Value.NULL);
            } else if (c instanceof Integer) {
                r.add(Value.of(((Integer) c).longValue()));
            } else if (c instanceof Long) {
                r.add(Value.of(((Long) c).longValue()));
            } else if (c instanceof Boolean) {
                r.add(Value.of(((Boolean) c).booleanValue()));
            } else {
                r.add(Value.of(String.valueOf(c)));
            }
        }
        return r;
    }

    /** JDBC LIKE 模式匹配（% 任意串 / _ 单字符；null 模式 = 全匹配）。 */
    private static boolean matches(String pattern, String value) {
        if (pattern == null) {
            return true;
        }
        return like(pattern.toUpperCase(java.util.Locale.ROOT), value == null ? ""
                : value.toUpperCase(java.util.Locale.ROOT), 0, 0);
    }

    private static boolean like(String p, String v, int pi, int vi) {
        while (pi < p.length()) {
            char c = p.charAt(pi);
            if (c == '%') {
                if (pi == p.length() - 1) {
                    return true;
                }
                for (int k = vi; k <= v.length(); k++) {
                    if (like(p, v, pi + 1, k)) {
                        return true;
                    }
                }
                return false;
            }
            if (vi >= v.length()) {
                return false;
            }
            if (c != '_' && c != v.charAt(vi)) {
                return false;
            }
            pi++;
            vi++;
        }
        return vi == v.length();
    }

    /** schema 过滤：null / "default" 匹配，其余不匹配（schema 概念单形 default）。 */
    private static boolean schemaMatches(String pattern) {
        return pattern == null || "default".equalsIgnoreCase(pattern);
    }

    /**
     * catalog 过滤（P2 多库）：null / 空串 / {@code "%"} = 全部；其余按 LIKE
     * 匹配库名（catalog ↔ database 映射）。
     */
    private static boolean catalogMatches(String catalog, String dbName) {
        if (catalog == null || catalog.isEmpty() || "%".equals(catalog)) {
            return true;
        }
        return matches(catalog, dbName == null ? "default" : dbName);
    }

    /**
     * 拆分 {@code /tables} 返回的表名（多挂载时为 {@code db.table} 限定形态，
     * 内核十六批 M-B）。返回 {@code [db, table]}；无限定前缀时 db 为 null
     * （单挂载/服务端 default 库）。
     */
    private static String[] splitQualified(String fullName) {
        int dot = fullName.indexOf('.');
        if (dot <= 0 || dot == fullName.length() - 1) {
            return new String[] {null, fullName};
        }
        return new String[] {fullName.substring(0, dot), fullName.substring(dot + 1)};
    }

    // ------------------------------------------------------------------
    // 表与列
    // ------------------------------------------------------------------

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                               String[] types) throws SQLException {
        if (!schemaMatches(schemaPattern)) {
            return rows(getTablesLayout(), emptyRows());
        }
        if (types != null && types.length > 0) {
            boolean wantTable = false;
            for (String t : types) {
                if ("TABLE".equalsIgnoreCase(t) || "ALL".equalsIgnoreCase(t)) {
                    wantTable = true;
                }
            }
            if (!wantTable) {
                return rows(getTablesLayout(), emptyRows());
            }
        }
        List<List<Value>> data = new ArrayList<List<Value>>();
        for (String fullName : connection.cachedTables()) {
            String[] qt = splitQualified(fullName);
            if (!catalogMatches(catalog, qt[0])) {
                continue;
            }
            if (matches(tableNamePattern, qt[1])) {
                // TABLE_CAT = 库名（无限定前缀 → 服务端 default 库）
                data.add(row(qt[0] != null ? qt[0] : "default", null, qt[1], "TABLE",
                        null, null, null, null, null, null));
            }
        }
        return rows(getTablesLayout(), data);
    }

    private static List<ColMeta> getTablesLayout() {
        return Arrays.asList(
                col("TABLE_CAT", QsdbType.VARCHAR),
                col("TABLE_SCHEM", QsdbType.VARCHAR),
                col("TABLE_NAME", QsdbType.VARCHAR),
                col("TABLE_TYPE", QsdbType.VARCHAR),
                col("REMARKS", QsdbType.VARCHAR),
                col("TYPE_CAT", QsdbType.VARCHAR),
                col("TYPE_SCHEM", QsdbType.VARCHAR),
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("SELF_REFERENCING_COL_NAME", QsdbType.VARCHAR),
                col("REF_GENERATION", QsdbType.VARCHAR));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        List<List<Value>> data = new ArrayList<List<Value>>();
        if (!schemaMatches(schemaPattern)) {
            return rows(getColumnsLayout(), data);
        }
        // J6：批量拉取（GET /tables?withSchema=1 单请求），消除 N+1
        java.util.Map<String, TableSchema> schemas = connection.allSchemas();
        for (java.util.Map.Entry<String, TableSchema> e : schemas.entrySet()) {
            String[] qt = splitQualified(e.getKey());
            if (!catalogMatches(catalog, qt[0])) {
                continue;
            }
            String table = qt[1];
            if (!matches(tableNamePattern, table)) {
                continue;
            }
            String tableCat = qt[0] != null ? qt[0] : "default";
            TableSchema schema = e.getValue();
            int ordinal = 0;
            for (TableSchema.Column c : schema.getColumns()) {
                ordinal++;
                if (!matches(columnNamePattern, c.name)) {
                    continue;
                }
                TypeStrings.Parsed p = TypeStrings.parse(c.type);
                int columnSize;
                int decimalDigits;
                switch (p.type) {
                    case DECIMAL:
                        columnSize = p.precision;
                        decimalDigits = p.scale;
                        break;
                    case DATE:
                        columnSize = 10;
                        decimalDigits = 0;
                        break;
                    case TIME:
                        columnSize = 18;
                        decimalDigits = 9;
                        break;
                    case TIMESTAMP:
                        columnSize = 29;
                        decimalDigits = 9;
                        break;
                    case BINARY:
                        columnSize = p.precision > 0 ? p.precision : 0;
                        decimalDigits = 0;
                        break;
                    default:
                        columnSize = p.precision > 0 ? p.precision : p.type.getDefaultPrecision();
                        decimalDigits = 0;
                }
                boolean isChar = p.type.isCharacter();
                data.add(row(
                        tableCat,                   // TABLE_CAT（库；P2 多库）
                        null,                       // TABLE_SCHEM
                        table,                      // TABLE_NAME（无限定名）
                        c.name,                     // COLUMN_NAME
                        p.type.getJdbcType(),       // DATA_TYPE
                        p.type.getJdbcTypeName(),   // TYPE_NAME
                        columnSize,                 // COLUMN_SIZE
                        null,                       // BUFFER_LENGTH
                        decimalDigits,              // DECIMAL_DIGITS
                        p.type.isNumeric() ? 10 : null, // NUM_PREC_RADIX
                        c.nullable ? columnNullable : columnNoNulls, // NULLABLE
                        null,                       // REMARKS
                        null,                       // COLUMN_DEF
                        null,                       // SQL_DATA_TYPE
                        null,                       // SQL_DATETIME_SUB
                        isChar ? columnSize : null, // CHAR_OCTET_LENGTH
                        ordinal,                    // ORDINAL_POSITION
                        c.nullable ? "YES" : "NO",  // IS_NULLABLE
                        null, null, null,           // SCOPE_*
                        null,                       // SOURCE_DATA_TYPE
                        "NO",                       // IS_AUTOINCREMENT
                        "NO"));                     // IS_GENERATEDCOLUMN
            }
        }
        return rows(getColumnsLayout(), data);
    }

    private static List<ColMeta> getColumnsLayout() {
        return Arrays.asList(
                col("TABLE_CAT", QsdbType.VARCHAR),
                col("TABLE_SCHEM", QsdbType.VARCHAR),
                col("TABLE_NAME", QsdbType.VARCHAR),
                col("COLUMN_NAME", QsdbType.VARCHAR),
                col("DATA_TYPE", QsdbType.INT),
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("COLUMN_SIZE", QsdbType.INT),
                col("BUFFER_LENGTH", QsdbType.INT),
                col("DECIMAL_DIGITS", QsdbType.INT),
                col("NUM_PREC_RADIX", QsdbType.INT),
                col("NULLABLE", QsdbType.INT),
                col("REMARKS", QsdbType.VARCHAR),
                col("COLUMN_DEF", QsdbType.VARCHAR),
                col("SQL_DATA_TYPE", QsdbType.INT),
                col("SQL_DATETIME_SUB", QsdbType.INT),
                col("CHAR_OCTET_LENGTH", QsdbType.INT),
                col("ORDINAL_POSITION", QsdbType.INT),
                col("IS_NULLABLE", QsdbType.VARCHAR),
                col("SCOPE_CATALOG", QsdbType.VARCHAR),
                col("SCOPE_SCHEMA", QsdbType.VARCHAR),
                col("SCOPE_TABLE", QsdbType.VARCHAR),
                col("SOURCE_DATA_TYPE", QsdbType.SMALLINT),
                col("IS_AUTOINCREMENT", QsdbType.VARCHAR),
                col("IS_GENERATEDCOLUMN", QsdbType.VARCHAR));
    }

    // ------------------------------------------------------------------
    // schema / catalog / 类型
    // ------------------------------------------------------------------

    /**
     * catalog 清单 = 挂载库清单（P2 多库：{@code GET /api/v1/databases}；
     * 非 admin 只列有 grant 的库）。旧 server（无该端点）降级为当前连接的
     * 目标库单行——工具树不至于空白。
     */
    @Override
    public ResultSet getCatalogs() throws SQLException {
        List<List<Value>> data = new ArrayList<List<Value>>();
        try {
            for (DbInfo db : connection.transport().databases()) {
                data.add(row(db.name));
            }
        } catch (QsdbException oldServer) {
            connection.markAuthFailedIfUnauthorized(oldServer);
            String fallback = connection.queryDatabase();
            if (fallback != null) {
                data.add(row(fallback));
            }
        }
        return rows(Arrays.asList(col("TABLE_CAT", QsdbType.VARCHAR)), data);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<List<Value>> data = new ArrayList<List<Value>>();
        // schema 概念单形 default（每个挂载库都有）；catalog 传具体库名时不做
        // 网络存在性核实——宽松放行，存在性由服务端在后续查询时裁决
        if (schemaMatches(schemaPattern)) {
            data.add(row("default", null)); // 内核 DEFAULT_DB（§八）
        }
        return rows(Arrays.asList(
                col("TABLE_SCHEM", QsdbType.VARCHAR),
                col("TABLE_CATALOG", QsdbType.VARCHAR)), data);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return rows(Arrays.asList(col("TABLE_TYPE", QsdbType.VARCHAR)),
                new ArrayList<List<Value>>(Arrays.asList(row("TABLE"))));
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<List<Value>> data = new ArrayList<List<Value>>();
        // 列：TYPE_NAME, DATA_TYPE, PRECISION, LITERAL_PREFIX, LITERAL_SUFFIX,
        //     CREATE_PARAMS, NULLABLE, CASE_SENSITIVE, SEARCHABLE, UNSIGNED_ATTRIBUTE,
        //     FIXED_PREC_SCALE, AUTO_INCREMENT, LOCAL_TYPE_NAME, MINIMUM_SCALE,
        //     MAXIMUM_SCALE, NUM_PREC_RADIX
        data.add(row("TINYINT", Types.TINYINT, 3, null, null, null, typeNullable, false, typeSearchable, false, false, false, "TINYINT", 0, 0, 10));
        data.add(row("SMALLINT", Types.SMALLINT, 5, null, null, null, typeNullable, false, typeSearchable, false, false, false, "SMALLINT", 0, 0, 10));
        data.add(row("INTEGER", Types.INTEGER, 10, null, null, null, typeNullable, false, typeSearchable, false, false, false, "INT", 0, 0, 10));
        data.add(row("BIGINT", Types.BIGINT, 19, null, null, null, typeNullable, false, typeSearchable, false, false, false, "BIGINT", 0, 0, 10));
        data.add(row("REAL", Types.REAL, 7, null, null, null, typeNullable, false, typeSearchable, false, false, false, "FLOAT", 0, 0, 10));
        data.add(row("DOUBLE", Types.DOUBLE, 15, null, null, null, typeNullable, false, typeSearchable, false, false, false, "DOUBLE", 0, 0, 10));
        data.add(row("DECIMAL", Types.DECIMAL, 38, null, null, "precision, scale", typeNullable, false, typeSearchable, false, true, false, "DECIMAL", 0, 38, 10));
        data.add(row("VARCHAR", Types.VARCHAR, 65535, "'", "'", "max length", typeNullable, true, typeSearchable, true, false, false, "STRING", 0, 0, 0));
        data.add(row("LONGVARCHAR", Types.LONGVARCHAR, Integer.MAX_VALUE, "'", "'", null, typeNullable, true, typeSearchable, true, false, false, "TEXT", 0, 0, 0));
        data.add(row("BOOLEAN", Types.BOOLEAN, 1, null, null, null, typeNullable, false, typeSearchable, false, false, false, "BOOLEAN", 0, 0, 0));
        data.add(row("DATE", Types.DATE, 10, "DATE '", "'", null, typeNullable, false, typePredBasic, false, false, false, "DATE", 0, 0, 0));
        data.add(row("TIME", Types.TIME, 18, "TIME '", "'", null, typeNullable, false, typePredBasic, false, false, false, "TIME", 0, 9, 0));
        data.add(row("TIMESTAMP", Types.TIMESTAMP, 29, "TIMESTAMP '", "'", null, typeNullable, false, typePredBasic, false, false, false, "TIMESTAMP", 0, 9, 0));
        data.add(row("BINARY", Types.BINARY, 0, null, null, null, typeNullable, true, typePredBasic, false, false, false, "BINARY", 0, 0, 0));
        return rows(Arrays.asList(
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("DATA_TYPE", QsdbType.SMALLINT),
                col("PRECISION", QsdbType.INT),
                col("LITERAL_PREFIX", QsdbType.VARCHAR),
                col("LITERAL_SUFFIX", QsdbType.VARCHAR),
                col("CREATE_PARAMS", QsdbType.VARCHAR),
                col("NULLABLE", QsdbType.SMALLINT),
                col("CASE_SENSITIVE", QsdbType.BOOLEAN),
                col("SEARCHABLE", QsdbType.SMALLINT),
                col("UNSIGNED_ATTRIBUTE", QsdbType.BOOLEAN),
                col("FIXED_PREC_SCALE", QsdbType.BOOLEAN),
                col("AUTO_INCREMENT", QsdbType.BOOLEAN),
                col("LOCAL_TYPE_NAME", QsdbType.VARCHAR),
                col("MINIMUM_SCALE", QsdbType.SMALLINT),
                col("MAXIMUM_SCALE", QsdbType.SMALLINT),
                col("NUM_PREC_RADIX", QsdbType.INT)), data);
    }

    // ------------------------------------------------------------------
    // 空集方法族（表格式存储无此元数据，§八）
    // ------------------------------------------------------------------

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return rows(Arrays.asList(
                col("TABLE_CAT", QsdbType.VARCHAR),
                col("TABLE_SCHEM", QsdbType.VARCHAR),
                col("TABLE_NAME", QsdbType.VARCHAR),
                col("COLUMN_NAME", QsdbType.VARCHAR),
                col("KEY_SEQ", QsdbType.SMALLINT),
                col("PK_NAME", QsdbType.VARCHAR)), emptyRows());
    }

    private static List<ColMeta> importedKeysLayout() {
        return Arrays.asList(
                col("PKTABLE_CAT", QsdbType.VARCHAR),
                col("PKTABLE_SCHEM", QsdbType.VARCHAR),
                col("PKTABLE_NAME", QsdbType.VARCHAR),
                col("PKCOLUMN_NAME", QsdbType.VARCHAR),
                col("FKTABLE_CAT", QsdbType.VARCHAR),
                col("FKTABLE_SCHEM", QsdbType.VARCHAR),
                col("FKTABLE_NAME", QsdbType.VARCHAR),
                col("FKCOLUMN_NAME", QsdbType.VARCHAR),
                col("KEY_SEQ", QsdbType.SMALLINT),
                col("UPDATE_RULE", QsdbType.SMALLINT),
                col("DELETE_RULE", QsdbType.SMALLINT),
                col("FK_NAME", QsdbType.VARCHAR),
                col("PK_NAME", QsdbType.VARCHAR),
                col("DEFERRABILITY", QsdbType.SMALLINT));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return rows(importedKeysLayout(), emptyRows());
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return rows(importedKeysLayout(), emptyRows());
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException {
        return rows(importedKeysLayout(), emptyRows());
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                  boolean approximate) throws SQLException {
        return rows(Arrays.asList(
                col("TABLE_CAT", QsdbType.VARCHAR),
                col("TABLE_SCHEM", QsdbType.VARCHAR),
                col("TABLE_NAME", QsdbType.VARCHAR),
                col("NON_UNIQUE", QsdbType.BOOLEAN),
                col("INDEX_QUALIFIER", QsdbType.VARCHAR),
                col("INDEX_NAME", QsdbType.VARCHAR),
                col("TYPE", QsdbType.SMALLINT),
                col("ORDINAL_POSITION", QsdbType.SMALLINT),
                col("COLUMN_NAME", QsdbType.VARCHAR),
                col("ASC_OR_DESC", QsdbType.VARCHAR),
                col("CARDINALITY", QsdbType.INT),
                col("PAGES", QsdbType.INT),
                col("FILTER_CONDITION", QsdbType.VARCHAR)), emptyRows());
    }

    private static List<ColMeta> proceduresLayout() {
        return Arrays.asList(
                col("PROCEDURE_CAT", QsdbType.VARCHAR),
                col("PROCEDURE_SCHEM", QsdbType.VARCHAR),
                col("PROCEDURE_NAME", QsdbType.VARCHAR),
                col("RESERVED_1", QsdbType.VARCHAR),
                col("RESERVED_2", QsdbType.VARCHAR),
                col("RESERVED_3", QsdbType.VARCHAR),
                col("REMARKS", QsdbType.VARCHAR),
                col("PROCEDURE_TYPE", QsdbType.SMALLINT));
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return rows(proceduresLayout(), emptyRows());
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        return rows(Arrays.asList(
                col("PROCEDURE_CAT", QsdbType.VARCHAR),
                col("PROCEDURE_SCHEM", QsdbType.VARCHAR),
                col("PROCEDURE_NAME", QsdbType.VARCHAR),
                col("COLUMN_NAME", QsdbType.VARCHAR),
                col("COLUMN_TYPE", QsdbType.SMALLINT),
                col("DATA_TYPE", QsdbType.INT),
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("PRECISION", QsdbType.INT),
                col("LENGTH", QsdbType.INT),
                col("SCALE", QsdbType.SMALLINT),
                col("RADIX", QsdbType.SMALLINT),
                col("NULLABLE", QsdbType.SMALLINT),
                col("REMARKS", QsdbType.VARCHAR),
                col("COLUMN_DEF", QsdbType.VARCHAR),
                col("SQL_DATA_TYPE", QsdbType.INT),
                col("SQL_DATETIME_SUB", QsdbType.INT),
                col("CHAR_OCTET_LENGTH", QsdbType.INT),
                col("ORDINAL_POSITION", QsdbType.INT),
                col("IS_NULLABLE", QsdbType.VARCHAR),
                col("SPECIFIC_NAME", QsdbType.VARCHAR)), emptyRows());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        // 内置函数无枚举端点——空集 + 文档指引（驱动反馈 J6，内核已收编）
        return rows(Arrays.asList(
                col("FUNCTION_CAT", QsdbType.VARCHAR),
                col("FUNCTION_SCHEM", QsdbType.VARCHAR),
                col("FUNCTION_NAME", QsdbType.VARCHAR),
                col("REMARKS", QsdbType.VARCHAR),
                col("FUNCTION_TYPE", QsdbType.SMALLINT),
                col("SPECIFIC_NAME", QsdbType.VARCHAR)), emptyRows());
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern, String columnNamePattern)
            throws SQLException {
        return rows(Arrays.asList(
                col("FUNCTION_CAT", QsdbType.VARCHAR),
                col("FUNCTION_SCHEM", QsdbType.VARCHAR),
                col("FUNCTION_NAME", QsdbType.VARCHAR),
                col("COLUMN_NAME", QsdbType.VARCHAR),
                col("COLUMN_TYPE", QsdbType.SMALLINT),
                col("DATA_TYPE", QsdbType.INT),
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("PRECISION", QsdbType.INT),
                col("LENGTH", QsdbType.INT),
                col("SCALE", QsdbType.SMALLINT),
                col("RADIX", QsdbType.SMALLINT),
                col("NULLABLE", QsdbType.SMALLINT),
                col("REMARKS", QsdbType.VARCHAR),
                col("CHAR_OCTET_LENGTH", QsdbType.INT),
                col("ORDINAL_POSITION", QsdbType.INT),
                col("IS_NULLABLE", QsdbType.VARCHAR),
                col("SPECIFIC_NAME", QsdbType.VARCHAR)), emptyRows());
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                             int[] types) throws SQLException {
        return rows(Arrays.asList(
                col("TYPE_CAT", QsdbType.VARCHAR),
                col("TYPE_SCHEM", QsdbType.VARCHAR),
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("CLASS_NAME", QsdbType.VARCHAR),
                col("DATA_TYPE", QsdbType.INT),
                col("REMARKS", QsdbType.VARCHAR),
                col("BASE_TYPE", QsdbType.SMALLINT)), emptyRows());
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
        return rows(Arrays.asList(
                col("TYPE_CAT", QsdbType.VARCHAR),
                col("TYPE_SCHEM", QsdbType.VARCHAR),
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("SUPERTYPE_CAT", QsdbType.VARCHAR),
                col("SUPERTYPE_SCHEM", QsdbType.VARCHAR),
                col("SUPERTYPE_NAME", QsdbType.VARCHAR)), emptyRows());
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return rows(Arrays.asList(
                col("TABLE_CAT", QsdbType.VARCHAR),
                col("TABLE_SCHEM", QsdbType.VARCHAR),
                col("TABLE_NAME", QsdbType.VARCHAR),
                col("SUPERTABLE_NAME", QsdbType.VARCHAR)), emptyRows());
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return rows(Arrays.asList(
                col("TYPE_CAT", QsdbType.VARCHAR),
                col("TYPE_SCHEM", QsdbType.VARCHAR),
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("ATTR_NAME", QsdbType.VARCHAR),
                col("DATA_TYPE", QsdbType.INT),
                col("ATTR_TYPE_NAME", QsdbType.VARCHAR),
                col("ATTR_SIZE", QsdbType.INT),
                col("ATTR_DECIMAL_DIGITS", QsdbType.INT),
                col("NUM_PREC_RADIX", QsdbType.INT),
                col("NULLABLE", QsdbType.INT),
                col("REMARKS", QsdbType.VARCHAR),
                col("ATTR_DEF", QsdbType.VARCHAR),
                col("SQL_DATA_TYPE", QsdbType.INT),
                col("SQL_DATETIME_SUB", QsdbType.INT),
                col("CHAR_OCTET_LENGTH", QsdbType.INT),
                col("ORDINAL_POSITION", QsdbType.INT),
                col("IS_NULLABLE", QsdbType.VARCHAR),
                col("SCOPE_CATALOG", QsdbType.VARCHAR),
                col("SCOPE_SCHEMA", QsdbType.VARCHAR),
                col("SCOPE_TABLE", QsdbType.VARCHAR),
                col("SOURCE_DATA_TYPE", QsdbType.SMALLINT)), emptyRows());
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        return rows(Arrays.asList(
                col("TABLE_CAT", QsdbType.VARCHAR),
                col("TABLE_SCHEM", QsdbType.VARCHAR),
                col("TABLE_NAME", QsdbType.VARCHAR),
                col("COLUMN_NAME", QsdbType.VARCHAR),
                col("DATA_TYPE", QsdbType.INT),
                col("COLUMN_SIZE", QsdbType.INT),
                col("DECIMAL_DIGITS", QsdbType.INT),
                col("NUM_PREC_RADIX", QsdbType.INT),
                col("COLUMN_USAGE", QsdbType.VARCHAR),
                col("REMARKS", QsdbType.VARCHAR),
                col("CHAR_OCTET_LENGTH", QsdbType.INT),
                col("IS_NULLABLE", QsdbType.VARCHAR)), emptyRows());
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return rows(Arrays.asList(
                col("SCOPE", QsdbType.SMALLINT),
                col("COLUMN_NAME", QsdbType.VARCHAR),
                col("DATA_TYPE", QsdbType.INT),
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("COLUMN_SIZE", QsdbType.INT),
                col("BUFFER_LENGTH", QsdbType.INT),
                col("DECIMAL_DIGITS", QsdbType.SMALLINT),
                col("PSEUDO_COLUMN", QsdbType.SMALLINT)), emptyRows());
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                          boolean nullable) throws SQLException {
        return rows(Arrays.asList(
                col("SCOPE", QsdbType.SMALLINT),
                col("COLUMN_NAME", QsdbType.VARCHAR),
                col("DATA_TYPE", QsdbType.INT),
                col("TYPE_NAME", QsdbType.VARCHAR),
                col("COLUMN_SIZE", QsdbType.INT),
                col("BUFFER_LENGTH", QsdbType.INT),
                col("DECIMAL_DIGITS", QsdbType.SMALLINT),
                col("PSEUDO_COLUMN", QsdbType.SMALLINT)), emptyRows());
    }

    private static List<ColMeta> privilegesLayout() {
        return Arrays.asList(
                col("TABLE_CAT", QsdbType.VARCHAR),
                col("TABLE_SCHEM", QsdbType.VARCHAR),
                col("TABLE_NAME", QsdbType.VARCHAR),
                col("GRANTOR", QsdbType.VARCHAR),
                col("GRANTEE", QsdbType.VARCHAR),
                col("PRIVILEGE", QsdbType.VARCHAR),
                col("IS_GRANTABLE", QsdbType.VARCHAR));
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return rows(privilegesLayout(), emptyRows());
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {
        return rows(Arrays.asList(
                col("TABLE_CAT", QsdbType.VARCHAR),
                col("TABLE_SCHEM", QsdbType.VARCHAR),
                col("TABLE_NAME", QsdbType.VARCHAR),
                col("COLUMN_NAME", QsdbType.VARCHAR),
                col("GRANTOR", QsdbType.VARCHAR),
                col("GRANTEE", QsdbType.VARCHAR),
                col("PRIVILEGE", QsdbType.VARCHAR),
                col("IS_GRANTABLE", QsdbType.VARCHAR)), emptyRows());
    }

    // ------------------------------------------------------------------
    // 产品/驱动信息
    // ------------------------------------------------------------------

    @Override
    public String getURL() throws SQLException {
        return connection.url(); // 脱敏后的 jdbc:qsdb://host:port（无 token）
    }

    @Override
    public String getUserName() throws SQLException {
        return "qsdb"; // §四.1 标签值
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return QsdbDriver.DATABASE_PRODUCT_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        if (cachedServerVersion == null) {
            cachedServerVersion = connection.serverVersion(); // /health 的 version 字段，失败回退
            if (cachedServerVersion == null) {
                cachedServerVersion = "unknown";
            }
        }
        return cachedServerVersion;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return versionPart(0);
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return versionPart(1);
    }

    private int versionPart(int idx) throws SQLException {
        String v = getDatabaseProductVersion();
        if ("unknown".equals(v)) {
            return 0;
        }
        String[] parts = v.split("\\.");
        if (idx >= parts.length) {
            return 0;
        }
        try {
            return Integer.parseInt(parts[idx].replaceAll("[^0-9].*$", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public String getDriverName() {
        return QsdbDriver.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() {
        return QsdbDriver.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return QsdbDriver.major();
    }

    @Override
    public int getDriverMinorVersion() {
        return QsdbDriver.minor();
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 2; // JDBC 4.2 规范目标
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public String getSQLKeywords() {
        return ""; // 无标准 SQL 之外的关键字保留集需要声明
    }

    @Override
    public String getNumericFunctions() {
        return "";
    }

    @Override
    public String getStringFunctions() {
        return "";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    // ------------------------------------------------------------------
    // 能力面（supports*：如实，不吹牛——§八）
    // ------------------------------------------------------------------

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        // 2026-10-13 起驱动 SQL 能力面含写入（INSERT/分区 DELETE/DDL）——
        // 如实报告连接提示（默认 false=可写），不再恒 true
        try {
            return connection.isReadOnly();
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public boolean usesLocalFiles() {
        return false; // server 侧文件对客户端不可见
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false; // 未验证——表格式 append/overwrite 语义下保守回答
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return false; // 未验证全类型 CONVERT 矩阵——保守
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false; // §八 明示
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true; // schema 端点有 nullable=false
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return true; // 内核 join 支持（M4 面）
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false; // 未验证 FULL JOIN——保守
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true; // LEFT JOIN
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false; // §八 明示
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return true; // 内核 translate.rs Expr::Exists
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return true; // InSubquery 已实现
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false; // ANY/ALL 未验证
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false; // 未验证外层引用——保守
    }

    @Override
    public boolean supportsUnion() {
        return true; // SetOperator::Union 已实现
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return true; // 无事务语义，语句天然跨"提交"
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return true;
    }

    @Override
    public boolean supportsTransactions() {
        return false; // §八 明示
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        // 快照读语义下 READ_COMMITTED 与 NONE 等价（§九）
        return level == Connection.TRANSACTION_NONE
                || level == Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return true; // 2026-10-13：executeBatch 逐条执行实现（诚实声明：无服务端批量协议）
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false; // 无行级标识回执
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY
                && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean supportsRefCursors() {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    // ------------------------------------------------------------------
    // 限制值（未知/无限制 → 0，规范语义）
    // ------------------------------------------------------------------

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public long getMaxLogicalLobSize() {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public java.sql.ResultSet getClientInfoProperties() throws SQLException {
        return rows(Arrays.asList(
                col("NAME", QsdbType.VARCHAR),
                col("MAX_LEN", QsdbType.INT),
                col("DEFAULT_VALUE", QsdbType.VARCHAR),
                col("DESCRIPTION", QsdbType.VARCHAR)), emptyRows());
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return true;
    }

    @Override
    public Connection getConnection() {
        return connection;
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
