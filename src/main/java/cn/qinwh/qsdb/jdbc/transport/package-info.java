/**
 * Transport SPI（设计文档 §三.4）：传输无关的查询模型与传输层抽象。
 * v1 只有 {@code transport.http.HttpTransport}；v2 的 Flight 传输实现同一
 * {@link cn.qinwh.qsdb.jdbc.transport.Transport} 接口即插即用，JDBC 对外接口零变化。
 */
package cn.qinwh.qsdb.jdbc.transport;
