package cn.qinwh.qsdb.jdbc.transport;

import java.util.Collections;
import java.util.List;

/**
 * {@code GET /api/v1/tables/{t}/schema} 的响应模型（§五.2 契约）。
 *
 * <p>列类型串为 LogicalType 方言（与 query 响应的 Arrow 方言不同——
 * 归一在核心层统一进行）。
 *
 * <p>不可变。
 */
public final class TableSchema {

    /** 单列定义。 */
    public static final class Column {
        public final String name;
        public final String type;
        public final boolean nullable;
        public final boolean partition;

        public Column(String name, String type, boolean nullable, boolean partition) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.partition = partition;
        }

        @Override
        public String toString() {
            return name + " " + type + (nullable ? " (nullable)" : " (not null)")
                    + (partition ? " [partition]" : "");
        }
    }

    private final String table;
    private final List<Column> columns;

    public TableSchema(String table, List<Column> columns) {
        this.table = table;
        this.columns = Collections.unmodifiableList(columns);
    }

    public String getTable() {
        return table;
    }

    public List<Column> getColumns() {
        return columns;
    }
}
