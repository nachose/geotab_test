package com.jiss.example.geotab.model;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Represents a fully enriched vehicle data record, combining LogRecord and StatusData (Odometer).
 * This will be the format for the final CSV output.
 */
public class EnrichedLogRecord {
    private String vehicleId;
    private String vin;
    private OffsetDateTime timestamp;
    private Double latitude;
    private Double longitude;
    private Double odometerValue;

    // Use the same formatter as in GeotabDataService for consistency in CSV output
    private static final DateTimeFormatter ISO_Z_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    public EnrichedLogRecord(String vehicleId, String vin, OffsetDateTime timestamp,
                             Double latitude, Double longitude, Double odometerValue) {
        this.vehicleId = vehicleId;
        this.vin = vin;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.odometerValue = odometerValue;
    }

    public String getVehicleId() { return vehicleId; }
    public String getVin() { return vin; }
    public OffsetDateTime getTimestamp() { return timestamp; }
    public Double getLatitude() { return latitude; }
    public Double getLongitude() { return longitude; }
    public Double getOdometerValue() { return odometerValue; }

    /**
     * Converts the EnrichedLogRecord into a String array suitable for CSVWriter.
     * Handles potential null values gracefully.
     */
    public String[] toCsvRow() {
        return new String[]{
                vehicleId,
                vin != null ? vin : "",
                timestamp != null ? timestamp.format(ISO_Z_FORMATTER) : "",
                latitude != null ? String.valueOf(latitude) : "",
                longitude != null ? String.valueOf(longitude) : "",
                odometerValue != null ? String.valueOf(odometerValue) : ""
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichedLogRecord that = (EnrichedLogRecord) o;
        // For deduplication and merging, vehicleId and timestamp form a logical key
        return Objects.equals(vehicleId, that.vehicleId) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vehicleId, timestamp);
    }

    @Override
    public String toString() {
        return "EnrichedLogRecord{" +
                "vehicleId='" + vehicleId + '\'' +
                ", vin='" + vin + '\'' +
                ", timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", odometerValue=" + odometerValue +
                '}';
    }
}
