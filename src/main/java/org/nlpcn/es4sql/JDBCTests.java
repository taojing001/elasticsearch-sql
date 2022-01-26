package org.nlpcn.es4sql;

import v10.com.alibaba.druid.pool.DruidDataSource;
import v10.com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class JDBCTests {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("url", "jdbc:elasticsearch://127.0.0.1:9300");
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        dds.setInitialSize(1);
        Connection connection = dds.getConnection();
        PreparedStatement ps = connection.prepareStatement("select count(1),avg(tenantId) as sessionCount from session_survey group by tenantId,channelId,date_histogram(field='createTime','interval'='1d')");
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getDouble("sessionCount"));
        }

        ps.close();
        connection.close();
        dds.close();
    }
}
