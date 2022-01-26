package org.nlpcn.es4sql.jdbc;

import v10.com.alibaba.druid.pool.DruidDataSource;
import v10.com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class EsDriver implements Driver {

    static
    {
        try {
            DriverManager.registerDriver(new EsDriver());
        } catch (SQLException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }

    }

    public Connection connect(String url, Properties info) throws SQLException {
        Properties properties = new Properties();
        properties.put("url", url);
        DruidDataSource dds=null;

        try {
            dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }

        assert dds != null;
        dds.setInitialSize(1);
        return dds.getConnection();
    }

    public boolean acceptsURL(String url) throws SQLException {
        // TODO 自动生成的方法存根
        if (!url.startsWith("jdbc:elasticsearch:"))
            return false;
        else
            return true;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // TODO 自动生成的方法存根
        return null;
    }

    public int getMajorVersion() {
        // TODO 自动生成的方法存根
        return 0;
    }

    public int getMinorVersion() {
        // TODO 自动生成的方法存根
        return 0;
    }

    public boolean jdbcCompliant() {
        // TODO 自动生成的方法存根
        return false;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO 自动生成的方法存根
        return null;
    }
}
