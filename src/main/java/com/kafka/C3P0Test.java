package com.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;

public class C3P0Test {

    private static Connection conn = null;
    private static PreparedStatement ps = null;
    private static ResultSet rs = null;
    public static HashSet<EmpGPS> empGPSSet = new HashSet<>();

    public static void main(String[] args) {
        // 1.批量插入操作
         getData();
        insertData();
//        insertData();
        // 2.批量更新操作
        // getDataByMySQL();
        // System.out.println(userSet.toString());
        // updateData();
        //3.批量删除操作
        // getDataByMySQL();
        // System.out.println(userSet.toString());
        // deleteData();
    }

//    // 查询数据库所有User数据
//    private static void getDataByMySQL() {
//        try {
//            int count = 0;
//            conn = C3P0Utils.getConnection();
//            conn.setAutoCommit(false);
//            String sql = "select * from USER";
//            ps = conn.prepareStatement(sql);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                User user = new User();
//                user.setHisuid(rs.getString(1));
//                user.setName(rs.getString(2));
//                user.setOld(rs.getInt(3));
//                user.setSex(rs.getString(4));
//                userSet.add(user);
//                count++;
//            }
//            System.out.println("查询到" + count + "条数据");
//        } catch (Exception e) {
//            try {
//                conn.rollback();
//            } catch (SQLException e1) {
//                e1.printStackTrace();
//            }
//            e.printStackTrace();
//        } finally {
//            try {
//                C3P0Utils.close(conn);
//                if (ps != null) {
//                    ps.close();
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//
//    }

    // 初始化empGPSSet
    public static void getData() {

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

        for(int i=0;i<10;i++){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10));
            System.out.println(records);
            for (ConsumerRecord<String, String> record : records) {
                //System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                //{"before":null,"after":{"HisUid":20191104092565508,"EmployeeID":"1432","EmployeeName":"张阳","GpsTime":1572859397000,"Longitude":30.36314,"Latitude":120.2156272,"Speed":11.1,"Angle":62.6,"VsTime":1572859514000},"source":{"version":"1.1.2.Final","connector":"sqlserver","name":"sqlserver_xj","ts_ms":1606143115835,"snapshot":"true","db":"IPMS4S_HRXJ_JK","schema":"dbo","table":"Emp_GpsDtata","change_lsn":null,"commit_lsn":"000052f2:00000538:001f","event_serial_no":null},"op":"r","ts_ms":1606143115835,"transaction":null}
                //String[] split = record.value().split(":\\{");
                //"HisUid":20191104092565508,"EmployeeID":"1432","EmployeeName":"张阳","GpsTime":1572859397000,"Longitude":30.36314,"Latitude":120.2156272,"Speed":11.1,"Angle":62.6,"VsTime":1572859514000},"source":{"version":"1.1.2.Final","connector":"sqlserver","name":"sqlserver_xj","ts_ms":1606143115835,"snapshot":"true","db":"IPMS4S_HRXJ_JK","schema":"dbo","table":"Emp_GpsDtata","change_lsn":null,"commit_lsn":"000052f2:00000538:001f","event_serial_no":null},"op":"r","ts_ms":1606143115835,"transaction":null}
               // String[] split1 = split[1].split(",");

                //将json字符串转换成jsonObject对象
                String myJsonObj = record.value();
                JSONObject jsonobj = JSON.parseObject(myJsonObj);
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                //json字段解析
                String hisUid = jsonobj.getJSONObject("after").getString("HisUid");
                String employeeID = jsonobj.getJSONObject("after").getString("EmployeeID");
                String employeeName = jsonobj.getJSONObject("after").getString("EmployeeName");

                Long gpsTime_timestamp = jsonobj.getJSONObject("after").getLong("GpsTime");
                Timestamp ts = new Timestamp(gpsTime_timestamp);
                String gpsTime = dateFormat.format(ts);

                Double longitude = jsonobj.getJSONObject("after").getDouble("Longitude");
                Double latitude = jsonobj.getJSONObject("after").getDouble("Latitude");
                Double speed = jsonobj.getJSONObject("after").getDouble("Speed");
                Double angle = jsonobj.getJSONObject("after").getDouble("Angle");

                Long vsTime_timestamp = jsonobj.getJSONObject("after").getLong("VsTime");
                Timestamp ts1 = new Timestamp(vsTime_timestamp);
                String vsTime = dateFormat.format(ts1);

                java.util.Date now = new Date();
                String update_time = dateFormat.format(now);

                EmpGPS u1 = new EmpGPS(hisUid, employeeID, employeeName, gpsTime, longitude, latitude, speed, angle, vsTime, update_time);
                empGPSSet.add(u1);
                System.out.println("22");
            }
        }
        System.out.println(empGPSSet);
    }
        // 批量插入数据
        public static void insertData(){
            try {
                int count = 0;
                conn = C3P0Utils.getConnection();
                conn.setAutoCommit(false);
                String sql = "insert into security_emp_gpsdtata_mi_v2 (hisuid,employeeid,employeename,gpstime,longitude,latitude,speed,angle,vstime,update_time) values(?,?,?,?,?,?,?,?,?,?)";
                ps=conn.prepareStatement(sql);
                for (EmpGPS empGPS : empGPSSet) {
                    ps.setString(1, empGPS.getHisuid());
                    ps.setString(2, empGPS.getEmployeeid());
                    ps.setString(3, empGPS.getEmployeename());
                    ps.setString(4, empGPS.getGpstime());
                    ps.setDouble(5, empGPS.getLongitude());
                    ps.setDouble(6, empGPS.getLatitude());
                    ps.setDouble(7, empGPS.getSpeed());
                    ps.setDouble(8, empGPS.getAngle());
                    ps.setString(9, empGPS.getVstime());
                    ps.setString(10, empGPS.getUpdate_time());
                    ps.addBatch();
                    count++;

                }
                System.out.println(count);
                ps.executeBatch();
//                ps.clearBatch();
                conn.commit();
                System.out.println("添加了" + count + "条数据");
            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();
            } finally {
                try {
                    C3P0Utils.close(rs,ps,conn);
                    if (ps != null) {
                        ps.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }

}