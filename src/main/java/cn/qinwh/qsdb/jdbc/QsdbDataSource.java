package cn.qinwh.qsdb.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * {@link DataSource} 实现（v1 提供，便于 Spring 注入，设计文档 §三.2）。
 *
 * <p>极薄：持有 URL/属性，{@code getConnection} 委托 {@link QsdbDriver}。
 * 线程安全；连接池化交给用户侧 Druid/HikariCP（驱动不内置池，§十一）。
 *
 * <p>属性：{@code setUrl}（{@code jdbc:qsdb://host:port[?props]}）、
 * {@code setPassword}/{@code setToken}（Bearer token，二者等价）、
 * {@code setUser}（仅标签用途）。URL 参数与 Properties 属性由
 * {@link QsdbDriver} 统一合并（URL 优先）。
 */
public final class QsdbDataSource implements DataSource {

    private final QsdbDriver driver = new QsdbDriver();
    private final Properties props = new Properties();
    private String url;
    private PrintWriter logWriter;

    public QsdbDataSource() {
    }

    public QsdbDataSource(String url) {
        this.url = url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    /** Bearer token（server --token）。 */
    public void setToken(String token) {
        props.setProperty("token", token);
    }

    /** {@code token} 的别名——便于连接池配置复用（§四.1）。 */
    public void setPassword(String password) {
        props.setProperty("password", password);
    }

    /** 标签用途（v1 不参与认证）。 */
    public void setUser(String user) {
        props.setProperty("user", user);
    }

    /** 附加驱动属性（connectTimeoutMs/queryTimeoutMs/maxRows 等）。 */
    public void setProperty(String name, String value) {
        props.setProperty(name, value);
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (url == null || url.isEmpty()) {
            throw new SQLException("url is not set; call setUrl(\"jdbc:qsdb://host:port\") first", "HY000");
        }
        return driver.connect(url, props);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        // user 仅标签用途；password 即 token（§四.1）
        Properties merged = new Properties();
        merged.putAll(props);
        if (username != null) {
            merged.setProperty("user", username);
        }
        if (password != null) {
            if (!merged.containsKey("token")) {
                merged.setProperty("password", password);
            }
        }
        if (url == null || url.isEmpty()) {
            throw new SQLException("url is not set; call setUrl(\"jdbc:qsdb://host:port\") first", "HY000");
        }
        return driver.connect(url, merged);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("does not implement '" + iface.getName() + "'", "HY000");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) {
        // 映射为 connectTimeoutMs（有显式设置则不覆盖）
        if (!props.containsKey("connectTimeoutMs")) {
            props.setProperty("connectTimeoutMs", String.valueOf(Math.max(0, seconds) * 1000));
        }
    }

    @Override
    public int getLoginTimeout() {
        String v = props.getProperty("connectTimeoutMs", "5000");
        try {
            return Integer.parseInt(v) / 1000;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(
                "java.util.logging is not used by this driver", "0A000");
    }
}
