package cn.qinwh.qsdb.jdbc.transport;

/**
 * 查询响应的列元数据（设计文档 §三.4）。
 *
 * <p>只携带内核透出的原始信息：列名与类型串（Arrow 方言）。JDBC 类型
 * 判定由核心层经 TypeStrings 统一进行——传输层不解释类型。
 *
 * <p>不可变。
 */
public final class ColMeta {

    private final String name;
    private final String typeName;

    public ColMeta(String name, String typeName) {
        this.name = name;
        this.typeName = typeName;
    }

    /** 列名（SELECT 别名经内核透出，{@code getColumnLabel} 同值）。 */
    public String getName() {
        return name;
    }

    /** 原始类型串（Arrow 方言，如 {@code Decimal128(10, 2)}）。 */
    public String getTypeName() {
        return typeName;
    }

    @Override
    public String toString() {
        return name + " " + typeName;
    }
}
