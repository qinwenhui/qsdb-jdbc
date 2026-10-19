/**
 * QSDB 官方 JDBC 驱动（Type 4 纯 Java，零第三方依赖，Java 8 起步）。
 *
 * <p>入口 {@link cn.qinwh.qsdb.jdbc.QsdbDriver}；协议契约与 SQL 能力面由
 * <a href="https://github.com/qinwenhui/qsdb">QSDB 内核</a>裁决，驱动透传
 * 不改写。
 *
 * <p>包结构：
 * <ul>
 *   <li>{@code cn.qinwh.qsdb.jdbc} — JDBC SPI 层（Driver/Connection/Statement/ResultSet/MetaData）</li>
 *   <li>{@code cn.qinwh.qsdb.jdbc.types} — QSDB ↔ JDBC 类型映射（{@code QsdbType}）</li>
 *   <li>{@code cn.qinwh.qsdb.jdbc.transport} — Transport SPI（v1: HTTP；v2: Flight 预留）</li>
 *   <li>{@code cn.qinwh.qsdb.jdbc.internal} — 参数编码 / JSON 解析 / HTTP 客户端（内建，不外泄）</li>
 * </ul>
 */
package cn.qinwh.qsdb.jdbc;
