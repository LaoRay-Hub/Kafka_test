package com.kafka;

import java.sql.*;

public class MysqlTest {private static Connection dbConn = null;

    public static void main(String[] args) {
        String URL = "jdbc:mysql://192.168.1.18:3306/hzgas";//数据库路径
        String name = "hzgas";                                                            //数据库账号
        String password = "Hzgas@2019";//数据库密码
        String select = "select * from  security_emp_gpsdtata_mi";//简单查询语句
        try {
            //1.加载驱动

//            Class.forName("com.mysql.jdbc.Driver");

            //2.连接
            dbConn = DriverManager.getConnection(URL, name, password);
            System.out.println("连接数据库成功！");
            PreparedStatement statement = null;

            statement = dbConn.prepareStatement(select);

            ResultSet res = null;
            res = statement.executeQuery();
            //当查询下一行有记录时：res.next()返回值为true，反之为false
            while (res.next()) {
                Long hisUid = res.getLong("HisUid");
                String employeeID = res.getString("EmployeeID");
                String employeeName = res.getString("EmployeeName");
                Timestamp gpsTime = res.getTimestamp("GpsTime");
                double longitude = res.getDouble("Longitude");
                double latitude = res.getDouble("Latitude");
                System.out.println("主键id:"+hisUid+"员工id：" + employeeID + "员工名称：" + employeeName + " Gps上传时间：" + gpsTime
                        +"经度"+longitude+"纬度"+latitude);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("连接数据库失败！");
        }
    }

}
