/**
 * 驱动内建实现（设计文档 §三.2）：参数渲染 / JSON 解析 / HTTP 客户端 /
 * 类型串归一 / 错误映射。
 *
 * <p><b>非公开 API</b>——本包类不进驱动公开面：Java 8 无模块系统，
 * 类可见性上必须是 public 才能被 {@code cn.qinwh.qsdb.jdbc} 核心层使用，
 * 但语义上属驱动内部实现，随时可能变化，业务代码不得直接依赖
 * （文档偏差回写：§三.2「package-private」在跨包需求下的落地方式）。
 */
package cn.qinwh.qsdb.jdbc.internal;
