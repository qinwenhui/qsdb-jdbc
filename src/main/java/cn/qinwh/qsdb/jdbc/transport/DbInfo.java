package cn.qinwh.qsdb.jdbc.transport;

/**
 * 挂载库信息（内核十六批 M-B：{@code GET /api/v1/databases} 的行形态；
 * 十八批Ⅱ 多库——库名进 SQL 限定符 {@code app.t}、{@code USE}、grants 键）。
 *
 * <p>非 admin 连接只列有 grant 的库（与 {@code SHOW DATABASES} 同口径）。
 * 不可变。
 */
public final class DbInfo {

    /** 库名（小写 {@code [a-z_][a-z0-9_]*}；固定挂载 {@code default}）。 */
    public final String name;

    /** 是否只读挂载（{@code --mount 名=路径,read-only}）。 */
    public final boolean readOnly;

    public DbInfo(String name, boolean readOnly) {
        this.name = name;
        this.readOnly = readOnly;
    }
}
