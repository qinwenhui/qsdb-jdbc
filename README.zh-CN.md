# qsdb-jdbc

**[QSDB](https://github.com/qinwenhui/qsdb) 官方 JDBC 驱动。Type 4 纯 Java，
单 jar 零依赖，Java 8 起步。**

qsdb-jdbc 是高性能分析数据库 QSDB 的官方 JDBC 驱动：Type 4 纯 Java 实现，
单 jar 零第三方依赖（HTTP 与 JSON 均内建）。Java 8 起步、JDBC 4.2 规范
目标——业务连 QSDB 与连 MySQL 写法零差异。

```java
Connection conn = DriverManager.getConnection(
    "jdbc:qsdb://qsdb-host:9430?token=xxx");
try (PreparedStatement ps = conn.prepareStatement(
        "SELECT DT, COUNT(*) AS n FROM orders WHERE DT > ? GROUP BY DT")) {
    ps.setDate(1, Date.valueOf("2026-10-01"));
    try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
            System.out.println(rs.getString(1) + " " + rs.getLong(2));
        }
    }
}
```

MyBatis / Spring JdbcTemplate / Druid / HikariCP / DBeaver / DataGrip /
帆软 FineReport 开箱可用。

## QSDB 生态

| 项目 | 说明 |
|---|---|
| [qsdb](https://github.com/qinwenhui/qsdb) | QSDB 数据库内核与 HTTP server |
| [qsdb-jdbc](https://github.com/qinwenhui/qsdb-jdbc) | 本 JDBC 驱动 |
| [qsdb-odbc](https://github.com/qinwenhui/qsdb-odbc) | ODBC 驱动 |
| [qsdb-sync](https://github.com/qinwenhui/qsdb-sync) | 实例间数据同步/复制 |
| [qsdb-studio](https://github.com/qinwenhui/qsdb-studio) | 可视化客户端工作台 |

## 连接

```text
jdbc:qsdb://host:port[?prop=value&prop=value]
```

| 属性 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `token` | String | — | 静态 Bearer token（server `--token`） |
| `user` + `password` | String | — | **login 会话**：Basic 换进程态 token；会话被踢自动重登（读安全重发一次/写只重登）；password 须与 user 成对 |
| `database` | String | — | 多库部署的默认库；等价 `Connection.setCatalog` |
| `connectTimeoutMs` | int | 5000 | TCP 建连超时 |
| `queryTimeoutMs` | int | 0（不限） | 查询默认超时；`Statement.setQueryTimeout` 逐语句覆盖 |
| `maxRows` | int | 0（不限） | 连接级默认行上限 |
| `readFormat` | String | `auto` | 查询响应格式：`auto`（优先二进制 rows 通道，旧 server 退回 json）\| `json` \| `rows` |
| `paramStyle` | String | `server` | 预处理参数通道：`server`（值随请求体进**内核绑定器**，从不进入 SQL 文本）\| `text`（客户端字面量渲染，服务旧内核） |
| `batchRewrite` | boolean | `true` | `executeBatch` 连续 INSERT 合并为多行 VALUES |
| `transport` | String | `http` | 本版本仅 http |
| `ssl` | boolean | `false` | 预留；须为 `false`（TLS 由反向代理终结） |

Spring 注入可用 `cn.qinwh.qsdb.jdbc.QsdbDataSource`（`setUrl` / `setToken`）。

## 特性

- **标准 JDBC 壳**：只进只读结果集、全类型 `getXxx`、`wasNull` 语义、
  **服务端参数化**——`?` 值随请求体进内核绑定器，**从不进入 SQL 文本**，
  注入防护由内核保证；BINARY / NaN / 控制字符参数全部可用
- **真流式大结果**：`Statement.setFetchSize(n>0)` 切到内核 rows 流式通道，
  逐块消费内存恒定；`setMaxRows` 截断即关流断链；`Statement.cancel()`
  支持传输段中止（两段停止语义）
- **SQL 能力面对齐引擎**：INSERT（VALUES）/ 分区粒度 DELETE / DDL 透传，
  `executeUpdate` 返回受影响行数；UPDATE/MERGE/REPLACE/UPSERT 显式拒绝
- **批量装载通道**：`QsdbBulkWriter` 走零依赖 `qsdb-rows` 二进制格式——
  100 万行 0.75s（约 130 万行/秒，比 SQL 通道快 13×）；`overwrite()`
  分区级替换、`overwriteCas(g)` 乐观并发（过期 generation 报
  `WRITE_CONFLICT`）
- **多库**：`?database=app` 或 `setCatalog("app")` 路由到挂载库；
  `getCatalogs()` 列挂载清单；元数据按 catalog 过滤
- **完整元数据**：DatabaseMetaData 表/列/类型方法 + `getTypeInfo`
- **传输层可插拔**：v1 HTTP（server 零配置）；接口为 Arrow Flight 预留
- **零依赖单 jar**：直接进业务微服务，无传递依赖

## 大批量装载

百万行级用批量通道，别用 `executeBatch`：

```java
try (QsdbBulkWriter w = QsdbBulkWriter.open(conn, "orders")) {
    for (Order o : orders) {
        w.setLong("id", o.id);
        w.setString("name", o.name);
        w.setDate("dt", o.dt);
        w.addRow();        // 攒够 65536 行自动编码 + 提交一块（内存恒定）
    }
}   // close() 提交剩余
```

## 构建与测试

```bash
mvn test                       # 单元测试（无需 server）
QSDB_IT_ADDR=127.0.0.1:9430 \  # 集成测试（门控；未设跳过）
QSDB_IT_TOKEN=xxx mvn test
mvn package                    # 单 jar
```

Java 8 单文件示例：[`examples/Demo.java`](examples/Demo.java)。

## 能力边界（v1 诚实声明）

- 无事务：autoCommit 恒 true，写语句逐条立即生效；UPDATE/MERGE/REPLACE/
  UPSERT 显式拒绝（0A000）
- **SQL 能力边界由内核裁决**（驱动透传，不改写）——权威清单见
  [qsdb](https://github.com/qinwenhui/qsdb) 仓库文档；分区表至少需一个非分区列
- 分页可用（`LIMIT/OFFSET`、`FETCH FIRST`、`NULLS FIRST/LAST`）——BI 工具
  可直接翻页；`setMaxRows` 是驱动侧截断，真正翻页用 SQL 端
- 不支持：`getGeneratedKeys`、Savepoint、游标滚动、LOB、SSL、Flight 传输
- `Statement.cancel()`：精确取消（`X-QSDB-Query-Id` 头预注册）
- 错误码：SQLState 映射运行期拉取 + 本地兜底；异常 message 带
  `[code=…, retry=…]` 注记（`TOO_MANY_QUERIES` 可退避重试、`READ_ONLY`
  须换实例）
- 建议配对 2026-10 之后的内核构建；旧 server 自动退回 JSON 读取与客户端
  字面量渲染

## 许可证

[MIT](LICENSE)
