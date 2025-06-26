package com.jiss.example.geotab.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.OffsetDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LogRecord {
    private String id;
    private OffsetDateTime dateTime;
    private Double latitude;
    private Double longitude;
    private Double odometer;

    public LogRecord() {
    }

    public LogRecord(String id, OffsetDateTime dateTime, Double latitude, Double longitude, Double odometer) {
        this.id = id;
        this.dateTime = dateTime;
        this.latitude = latitude;
        this.longitude = longitude;
        this.odometer = odometer;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public OffsetDateTime getDateTime() {
        return dateTime;
    }

    public void setDateTime(OffsetDateTime dateTime) {
        this.dateTime = dateTime;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getOdometer() {
        return odometer;
    }

    public void setOdometer(Double odometer) {
        this.odometer = odometer;
    }

    public boolean hasAllRequiredFields() {
        return dateTime != null && latitude != null && longitude != null && odometer != null;
    }
}
