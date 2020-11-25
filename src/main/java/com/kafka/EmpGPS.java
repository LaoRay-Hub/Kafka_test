package com.kafka;

import java.util.Objects;

public class EmpGPS {


    private String hisuid;
    private String employeeid;
    private String employeename;
    private String gpstime;
    private double longitude;
    private double latitude;
    private double speed;
    private double angle;
    private String vstime;
    private String update_time;

    public String getHisuid() {
        return hisuid;
    }

    public void setHisuid(String hisuid) {
        this.hisuid = hisuid;
    }

    public String getEmployeeid() {
        return employeeid;
    }

    public void setEmployeeid(String employeeid) {
        this.employeeid = employeeid;
    }

    public String getEmployeename() {
        return employeename;
    }

    public void setEmployeename(String employeename) {
        this.employeename = employeename;
    }

    public String getGpstime() {
        return gpstime;
    }

    public void setGpstime(String gpstime) {
        this.gpstime = gpstime;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public double getAngle() {
        return angle;
    }

    public void setAngle(double angle) {
        this.angle = angle;
    }

    public String getVstime() {
        return vstime;
    }

    public void setVstime(String vstime) {
        this.vstime = vstime;
    }

    public String getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(String update_time) {
        this.update_time = update_time;
    }

    @Override
    public String toString() {
        return "EmpGPS{" +
                "hisuid='" + hisuid + '\'' +
                ", employeeid='" + employeeid + '\'' +
                ", employeename='" + employeename + '\'' +
                ", gpstime='" + gpstime + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", speed=" + speed +
                ", angle=" + angle +
                ", vstime='" + vstime + '\'' +
                ", update_time='" + update_time + '\'' +
                '}';
    }

    public EmpGPS(String hisuid, String employeeid, String employeename, String gpstime, double longitude, double latitude, double speed, double angle, String vstime, String update_time) {
        super();
        this.hisuid = hisuid;
        this.employeeid = employeeid;
        this.employeename = employeename;
        this.gpstime = gpstime;
        this.longitude = longitude;
        this.latitude = latitude;
        this.speed = speed;
        this.angle = angle;
        this.vstime = vstime;
        this.update_time = update_time;
    }

    public EmpGPS() {
        super();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmpGPS empGPS = (EmpGPS) o;
        return Double.compare(empGPS.longitude, longitude) == 0 &&
                Double.compare(empGPS.latitude, latitude) == 0 &&
                Double.compare(empGPS.speed, speed) == 0 &&
                Double.compare(empGPS.angle, angle) == 0 &&
                hisuid.equals(empGPS.hisuid) &&
                employeeid.equals(empGPS.employeeid) &&
                employeename.equals(empGPS.employeename) &&
                gpstime.equals(empGPS.gpstime) &&
                vstime.equals(empGPS.vstime) &&
                update_time.equals(empGPS.update_time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hisuid, employeeid, employeename, gpstime, longitude, latitude, speed, angle, vstime, update_time);
    }
}