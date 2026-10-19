package cn.qinwh.qsdb.jdbc.transport;

/**
 * 传输无关的单元格值——判别联合（设计文档 §三.4）：null / long / double /
 * bool / string / bytes。
 *
 * <p>传输层不做 JDBC 类型判定：JSON 通道上值的 Java 形态由内核的显示串
 * 数字启发决定（如 DOUBLE 列的整数值会以 long 到达、VARCHAR 列的数值串
 * 会以 long 到达），核心层按列的 {@code QsdbType} 统一转换。
 *
 * <p>不可变；预分配 {@link #NULL} 单例。{@code bytes} 形态为 v2 IPC 通道
 * 预留（BINARY 列），v1 JSON 通道不产出。
 */
public final class Value {

    /** 判别种类。 */
    public enum Kind {
        NULL, LONG, DOUBLE, BOOL, STRING, BYTES
    }

    /** 单例 null 值。 */
    public static final Value NULL = new Value(Kind.NULL, null);

    private final Kind kind;
    private final Object v;

    private Value(Kind kind, Object v) {
        this.kind = kind;
        this.v = v;
    }

    public static Value of(long x) {
        return new Value(Kind.LONG, x);
    }

    public static Value of(double x) {
        return new Value(Kind.DOUBLE, x);
    }

    public static Value of(boolean x) {
        return new Value(Kind.BOOL, x);
    }

    public static Value of(String x) {
        return x == null ? NULL : new Value(Kind.STRING, x);
    }

    public static Value of(byte[] x) {
        return x == null ? NULL : new Value(Kind.BYTES, x);
    }

    public Kind getKind() {
        return kind;
    }

    public boolean isNull() {
        return kind == Kind.NULL;
    }

    public long asLong() {
        return (Long) v;
    }

    public double asDouble() {
        return (Double) v;
    }

    public boolean asBoolean() {
        return (Boolean) v;
    }

    public String asString() {
        return (String) v;
    }

    public byte[] asBytes() {
        return (byte[]) v;
    }

    @Override
    public String toString() {
        switch (kind) {
            case NULL:
                return "null";
            case LONG:
            case DOUBLE:
            case BOOL:
            case STRING:
                return String.valueOf(v);
            case BYTES:
                return "<bytes:" + ((byte[]) v).length + ">";
            default:
                return "?";
        }
    }
}
