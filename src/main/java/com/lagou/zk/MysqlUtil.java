package com.lagou.zk;


import java.io.PrintWriter;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;


import com.alibaba.druid.pool.DruidDataSource;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import javax.sql.DataSource;


public class MysqlUtil implements DataSource {



    public MysqlUtil(){
        initConfig();
        //第一次，节点数据改变的类型，接收通知后的处理逻辑定义
        zkClient.subscribeDataChanges("/mysqlDataSource", new IZkDataListener() {
            public void handleDataChange(String path, Object data) throws Exception {
                initConfig();
            }

            public void handleDataDeleted(String s) throws Exception {

            }
        });
    }

    //获取到zkClient
    private  ZkClient zkClient = new ZkClient("linux121:2181,linux122:2181");
    private   volatile DruidDataSource dataSource;
    private final static String defaultDriverClassName = "com.mysql.jdbc.Driver";
    private final static String defaultURL = "jdbc:mysql://linux123:3306/sys?useSSL=false";
    private final static String defaultUsername = "hive";
    private final static String defaultPassword = "12345678";


    /**
     * 添加mysql信息到properties，zk中有的直接取zk的，没有的就添加默认的到Properties，同时添加到zk
     * * @return Properties
     */
    public  void initConfig() {
        Properties properties = new Properties();
        //设置默认的数据库连接参数到zookeeper,并开启监听
        if (!zkClient.exists("/mysqlDataSource")) {
            zkClient.createPersistent("/mysqlDataSource");
            properties.put("driverClassName", defaultDriverClassName);
            properties.put("url", defaultURL);
            properties.put("username", defaultUsername);
            properties.put("password", defaultPassword);
            zkClient.createPersistent("/mysqlDataSource/driverClassName", defaultDriverClassName);
            zkClient.createPersistent("/mysqlDataSource/url", defaultURL);
            zkClient.createPersistent("/mysqlDataSource/username", defaultUsername);
            zkClient.createPersistent("/mysqlDataSource/password", defaultPassword);
        } else {
            //遍历mysqlDataSource节点下面所有信息
            List<String> childs = zkClient.getChildren("/mysqlDataSource");
            if (!childs.contains("driverClassName")) {
                zkClient.createPersistent("/mysqlDataSource/driverClassName", defaultDriverClassName);
                properties.put("driverClassName", defaultDriverClassName);
            } else {
                properties.put("driverClassName", zkClient.readData("/mysqlDataSource/driverClassName"));
            }
            if (!childs.contains("url")) {
                zkClient.createPersistent("/mysqlDataSource/url", defaultURL);
                properties.put("url", defaultURL);
            } else {
                properties.put("url", zkClient.readData("/mysqlDataSource/url"));
            }
            if (!childs.contains("username")) {
                zkClient.createPersistent("/mysqlDataSource/username", defaultUsername);
                properties.put("username", defaultUsername);
            } else {
                properties.put("username", zkClient.readData("/mysqlDataSource/username"));
            }
            if (!childs.contains("password")) {
                zkClient.createPersistent("/mysqlDataSource/password", defaultPassword);
                properties.put("password", defaultPassword);
            } else {
                properties.put("password", zkClient.readData("/mysqlDataSource/password"));
            }
        }

        DruidDataSource current=MysqlUtil.this.dataSource;
        try {
            DruidDataSource datasource2 = new  DruidDataSource();
            datasource2.setDriverClassName((String) properties.get("driverClassName"));
            datasource2.setUrl((String) properties.get("url"));
            datasource2.setUsername((String) properties.get("username"));
            datasource2.setPassword((String) properties.get("password"));

            // 将数据源切换为新的数据源
            MysqlUtil.this.dataSource = datasource2;
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(current!=null){
            current.close();
        }
    }

    /**
     * 修改zookpper信息，释放连接池，创建新的连接池
     *
     * @param properties
     */
    public  void updateMySqlInfoInzokeeper(Properties properties) {
        Set<Object> keys = properties.keySet();
        for (Object key : keys) {
            //修改zk配置
            zkClient.writeData("/mysqlDataSource/" + key, properties.get(key));
        }
    }

    /**
     * 创建数据库连接实例
     *
     * @return 数据库连接实例 connection
     */
    public   Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("获取数据库连接异常");
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return dataSource.getConnection(username, password);
    }


    /**
     * 释放数据库连接 connection 到数据库缓存池
     *
     * @param connection 数据库连接对象
     */
    public  void releaseSqlConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 释放数据库连接 connection 到数据库缓存池，并关闭 rSet 和 pStatement 资源
     *
     * @param rSet       数据库处理结果集
     * @param pStatement 数据库操作语句
     * @param connection 数据库连接对象
     */
    public static void releaseSqlConnection(ResultSet rSet, PreparedStatement pStatement, Connection connection) {
        try {
            if (rSet != null) {
                rSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (pStatement != null) {
                    pStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    public void setLoginTimeout(int seconds) throws SQLException {

    }

    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}