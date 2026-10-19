package cn.qinwh.qsdb.jdbc.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * 错误码本地兜底表单测（对齐内核 docs/error-codes.md §三，28 码——
 * 2026-10-19 补齐内核十六批～二十批新增的 5 码）。
 */
class ErrorCodeTableTest {

    @Test
    void allTwentyEightCodesPresent() {
        assertEquals(28, ErrorCodeTable.local().size(), "码表应与内核 §三 的 28 码对齐");
    }

    @Test
    void authPairSharesSqlStateButDivergesOnRecovery() {
        // 401 双码：同 28000 但动作不同——UNAUTHORIZED 凭证错不可自愈；
        // SESSION_REVOKED 持 login 凭证时重登即恢复（会话面按 code 分档）
        assertEquals("28000", ErrorCodeTable.lookup("UNAUTHORIZED").sqlState);
        ErrorCodeTable.Entry revoked = ErrorCodeTable.lookup("SESSION_REVOKED");
        assertEquals("28000", revoked.sqlState);
        assertFalse(revoked.retryable, "重发无意义——必须先重登（驱动会话面负责）");
    }

    @Test
    void resultGuardIsNotRetriable() {
        // 内核十七批 R1：结果护栏恒定拒绝、绝不返回部分数据——
        // 动作 = 加 LIMIT / 调大 maxRows / 换流式，不是可重试故障
        ErrorCodeTable.Entry guard = ErrorCodeTable.lookup("RESULT_TOO_LARGE");
        assertEquals("54000", guard.sqlState);
        assertFalse(guard.retryable);
    }

    @Test
    void cursorAndSegmentCodes() {
        // watch 游标过期：全量对账后从当前续订（重试同一游标无意义）
        assertEquals("HY000", ErrorCodeTable.lookup("CURSOR_TOO_OLD").sqlState);
        // 文件回收面：段被存活 manifest 引用，拒绝进回收站
        assertEquals("55006", ErrorCodeTable.lookup("SEGMENT_REFERENCED").sqlState);
    }

    @Test
    void conflictCodesShareSqlStateButDifferByCode() {
        // F24/F26：两个 409 码**同为 40001 但动作相反**——区分只能靠 code 本身
        ErrorCodeTable.Entry writeConflict = ErrorCodeTable.lookup("WRITE_CONFLICT");
        assertEquals("40001", writeConflict.sqlState);
        assertFalse(writeConflict.retryable, "盲目重试无意义（需重读 generation）");

        ErrorCodeTable.Entry staleWriter = ErrorCodeTable.lookup("STALE_WRITER");
        assertEquals("40001", staleWriter.sqlState);
        assertFalse(staleWriter.retryable, "重试无用（需抬高 epoch 重启）");
    }

    @Test
    void fiftyThreeSplitByCodeNotByStatus() {
        // 内核 §四 重点：同 503 三个码，语义相反——消费方必须按 code 分流
        ErrorCodeTable.Entry tooMany = ErrorCodeTable.lookup("TOO_MANY_QUERIES");
        assertEquals("53300", tooMany.sqlState);
        assertTrue(tooMany.retryable, "并发满可退避重试");

        ErrorCodeTable.Entry sessions = ErrorCodeTable.lookup("TOO_MANY_SESSIONS");
        assertEquals("08004", sessions.sqlState);
        assertTrue(sessions.retryable, "会话数上限：等既有会话退出后重试有意义");

        ErrorCodeTable.Entry pressure = ErrorCodeTable.lookup("MEMORY_PRESSURE");
        assertEquals("53000", pressure.sqlState);
        assertTrue(pressure.retryable, "全库内存压力可退避重试（他人占用）");

        ErrorCodeTable.Entry readOnly = ErrorCodeTable.lookup("READ_ONLY");
        assertEquals("25006", readOnly.sqlState);
        assertFalse(readOnly.retryable, "只读库永不重试，须换实例");
    }

    @Test
    void memoryLimitVsPressureAreOpposites() {
        // 400 QUERY_MEMORY_LIMIT（自己超配额，改查询）vs 503 MEMORY_PRESSURE
        // （他人占用预算，退避即可）——动作相反，勿混
        assertFalse(ErrorCodeTable.lookup("QUERY_MEMORY_LIMIT").retryable);
        assertTrue(ErrorCodeTable.lookup("MEMORY_PRESSURE").retryable);
    }

    @Test
    void driverContractOverrides() {
        // SQLState 归驱动所有：QUERY_TIMEOUT 恒 HYT00（§十 契约，非内核建议 57014）
        assertEquals("HYT00", ErrorCodeTable.lookup("QUERY_TIMEOUT").sqlState);
        assertEquals("HY008", ErrorCodeTable.lookup("QUERY_CANCELLED").sqlState);
    }

    @Test
    void semanticFamilies() {
        assertEquals("42P01", ErrorCodeTable.lookup("TABLE_NOT_FOUND").sqlState);
        assertEquals("42703", ErrorCodeTable.lookup("COLUMN_NOT_FOUND").sqlState);
        assertEquals("42601", ErrorCodeTable.lookup("PARSE_ERROR").sqlState);
        assertEquals("42000", ErrorCodeTable.lookup("INVALID_ARGUMENT").sqlState);
        assertEquals("28000", ErrorCodeTable.lookup("UNAUTHORIZED").sqlState);
        assertEquals("0A000", ErrorCodeTable.lookup("UNSUPPORTED").sqlState);
        assertEquals("08P01", ErrorCodeTable.lookup("INVALID_IPC").sqlState);
    }

    @Test
    void unknownCodeReturnsNull() {
        assertNull(ErrorCodeTable.lookup("SOME_FUTURE_CODE"));
        assertNull(ErrorCodeTable.lookup(null));
    }
}
