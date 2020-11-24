package com.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 * Author: zhengyuanlei
 * Date: 2020/11/24
 * Description:
 */

public class EmpGPSKafkaController {
    private static Connection dbConn = null;

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "prod-node4:9092");
        properties.put("group.id", "group-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);

        kafkaConsumer.subscribe(Arrays.asList("sqlserver_xj.dbo.Emp_GpsDtata"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                //System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                //{"before":null,"after":{"HisUid":20191104092565508,"EmployeeID":"1432","EmployeeName":"张阳","GpsTime":1572859397000,"Longitude":30.36314,"Latitude":120.2156272,"Speed":11.1,"Angle":62.6,"VsTime":1572859514000},"source":{"version":"1.1.2.Final","connector":"sqlserver","name":"sqlserver_xj","ts_ms":1606143115835,"snapshot":"true","db":"IPMS4S_HRXJ_JK","schema":"dbo","table":"Emp_GpsDtata","change_lsn":null,"commit_lsn":"000052f2:00000538:001f","event_serial_no":null},"op":"r","ts_ms":1606143115835,"transaction":null}
                String[] split = record.value().split(":\\{");
                //"HisUid":20191104092565508,"EmployeeID":"1432","EmployeeName":"张阳","GpsTime":1572859397000,"Longitude":30.36314,"Latitude":120.2156272,"Speed":11.1,"Angle":62.6,"VsTime":1572859514000},"source":{"version":"1.1.2.Final","connector":"sqlserver","name":"sqlserver_xj","ts_ms":1606143115835,"snapshot":"true","db":"IPMS4S_HRXJ_JK","schema":"dbo","table":"Emp_GpsDtata","change_lsn":null,"commit_lsn":"000052f2:00000538:001f","event_serial_no":null},"op":"r","ts_ms":1606143115835,"transaction":null}
                String[] split1 = split[1].split(",");

                //将json字符串转换成jsonObject对象
                String myJsonObj = record.value();
                JSONObject jsonobj = JSON.parseObject(myJsonObj);

                //json字段解析
                String hisUid = jsonobj.getJSONObject("after").getString("HisUid");
                String employeeID = jsonobj.getJSONObject("after").getString("EmployeeID");
                String employeeName = jsonobj.getJSONObject("after").getString("EmployeeName");
                Long gpsTime_timestamp = jsonobj.getJSONObject("after").getLong("GpsTime");
                Timestamp ts = new Timestamp(gpsTime_timestamp);
                Date gpsTime = new Date();
                gpsTime = ts;
                Double longitude = jsonobj.getJSONObject("after").getDouble("Longitude");
                Double latitude = jsonobj.getJSONObject("after").getDouble("Latitude");
                Double speed = jsonobj.getJSONObject("after").getDouble("Speed");
                Double angle = jsonobj.getJSONObject("after").getDouble("Angle");
                Long vsTime_timestamp = jsonobj.getJSONObject("after").getLong("VsTime");
                Timestamp ts1 = new Timestamp(vsTime_timestamp);
                Date vsTime = new Date();
                vsTime = ts1;
                Date now = new Date();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//可以方便地修改日期格式
                String update_time = dateFormat.format(now);

                System.out.println(hisUid);
                System.out.println(employeeID);
                System.out.println(employeeName);
                System.out.println(gpsTime);
                System.out.println(longitude);
                System.out.println(latitude);
                System.out.println(speed);
                System.out.println(angle);
                System.out.println(vsTime);
                System.out.println(update_time);


                String dbURL = "jdbc:mysql://192.168.1.18:3306/hzgas?characterEncoding=utf8";//数据库路径
                String name = "hzgas";                                                            //数据库账号
                String password = "Hzgas@2019";//数据库密码
                String insert = "insert into security_emp_gpsdtata_mi_v2 (hisuid,employeeID,employeeName,gpsTime,longitude,latitude,speed,angle,vsTime,update_time)\n" +
                        "values(?,?,?,?,?,?,?,?,?,?)";
                try {
                    //1.加载驱动
//                    Class.forName("com.mysql.cj.jdbc.Driver");

                    //2.连接
                    dbConn = DriverManager.getConnection(dbURL, name, password);
                    System.out.println("连接数据库成功！");

                    PreparedStatement pre = dbConn.prepareStatement(insert);
                    pre.setString(1, hisUid);
                    pre.setString(2, employeeID);
                    pre.setString(3, employeeName);
                    pre.setDate(4, (java.sql.Date) gpsTime);
                    pre.setDouble(5, longitude);
                    pre.setDouble(6, latitude);
                    pre.setDouble(7, speed);
                    pre.setDouble(8, angle);
                    pre.setDate(9, (java.sql.Date) vsTime);
                    pre.setString(10, update_time);


                    pre.execute();

                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("连接数据库失败！");
                }
            }

        }
    }
}

