package com.jiss.example.geotab.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.OffsetDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LogRecord {
    private double latitude;
    private double longitude;
    private int speed;
    private String dateTimeRaw;
    private OffsetDateTime dateTime;
    private DeviceRef device;
    private String id;

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public String getDateTimeRaw() {
        return dateTimeRaw;
    }

    public void setDateTime(String dateTimeRaw) {
        this.dateTimeRaw = dateTimeRaw;
        this.dateTime = OffsetDateTime.parse(dateTimeRaw);
    }

    public OffsetDateTime getDateTime() {
        return dateTime;
    }

    public DeviceRef getDevice() {
        return device;
    }

    public void setDevice(DeviceRef device) {
        this.device = device;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public static class DeviceRef {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    @Override
    public String toString() {
        return "LogRecord{" +
                "id='" + id + '\'' +
                ", dateTime=" + dateTime +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", speed=" + speed +
                ", deviceId='" + (device != null ? device.getId() : "null") + '\'' +
                '}';
    }

    public boolean hasAllRequiredFields() {
        return dateTime != null && device != null && device.getId() != null && latitude != 0 && longitude != 0;
    }
}
