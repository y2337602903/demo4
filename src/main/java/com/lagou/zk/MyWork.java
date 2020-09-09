package com.lagou.zk;



import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

public class MyWork {


    public static void main(String[] args) throws InterruptedException {
        MyWork myWork=new MyWork();
        MysqlUtil mysqlUtil=new MysqlUtil();

        //取得最开始的连接
        Connection connection = mysqlUtil.getConnection();
        //查看连接元数据
        myWork.getMetaData(connection);

        Properties properties = new Properties();
        properties.setProperty("url","jdbc:mysql://linux123:3306/hivemetadata?useSSL=false" );
        properties.setProperty("username","hive2" );
        properties.setProperty("password","12345678" );


        //关闭之前的连接，并重新初始化数据库连接池
        mysqlUtil.updateMySqlInfoInzokeeper(properties);
        //再次取得数据库连接
        Connection connection2 = mysqlUtil.getConnection();
        //再次查看连接元数据
        myWork.getMetaData(connection2);
    }

    public void getMetaData( Connection connection){
        DatabaseMetaData metaData = null;
        try {
            metaData = connection.getMetaData();
            String userName = metaData.getUserName();
            System.out.println("userName = " + userName);
            System.out.println("metaData = " + metaData);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
