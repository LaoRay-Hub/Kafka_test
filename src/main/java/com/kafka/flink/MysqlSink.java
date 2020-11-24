package com.kafka.flink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import scala.Tuple3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink extends RichSinkFunction<Tuple3<String, String, String>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "hzgas";
    String password = "Hzgas@2019";
    String drivername = "com.mysql.jdbc.Driver";   //配置改成自己的配置
    String dburl = "jdbc:mysql://192.168.1.18:3306/hzgas";

    @Override
    public void invoke(Tuple3<String, String, String> value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "replace into security_emp_gpsdtata_mi_v2(hisuid,employeeID,employeeName,gpsTime,longitude,latitude,speed,angle,vsTime,update_time) values(?,?,?,?,?,?,?,?,?,?)"; //假设mysql 有3列 id,num,price
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, value._1());
        preparedStatement.setString(2, value._2());
        preparedStatement.setString(3, value._3());
        preparedStatement.executeUpdate();


        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
