package cn.qinwh.qsdb.jdbc.transport;

/**
 * 分区信息（{@code GET /api/v1/tables/{t}/partitions} 的 {@code detail} 行）。
 *
 * <p>{@link #generation} 是分区提交计数器（每次提交 +1）——overwrite CAS
 * 写入的乐观并发判据：{@code expected_generation} 不匹配 → {@code
 * WRITE_CONFLICT}（动作 = 重读最新 generation 再决定，不是盲目重试）。
 * 不可变。
 */
public final class PartitionInfo {

    /** 分区名（分区列值目录形态）。 */
    public final String name;

    /** 提交计数器（CAS 写入的期望值来源）。 */
    public final long generation;

    /** 行数。 */
    public final long rows;

    /** 段文件数。 */
    public final long segments;

    /** 字节数。 */
    public final long bytes;

    public PartitionInfo(String name, long generation, long rows, long segments, long bytes) {
        this.name = name;
        this.generation = generation;
        this.rows = rows;
        this.segments = segments;
        this.bytes = bytes;
    }
}
