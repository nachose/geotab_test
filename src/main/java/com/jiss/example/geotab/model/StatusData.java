package com.jiss.example.geotab.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.OffsetDateTime;

/**
 * Represents a StatusData entry from the Geotab API.
 * This can represent various diagnostic readings, like odometer.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StatusData {
    public String id;
    public OffsetDateTime dateTime;
    public Double data;             // The value of the status data (e.g., odometer value)
    public DeviceReference device;  // Reference to the Device this status belongs to
    public Diagnostic diagnostic;   // To identify the type of status (e.g., Odometer)

    // Nested class for Diagnostic reference (ID only)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Diagnostic {
        public String id; // e.g., "DiagnosticOdometerId"
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
    }

    // Nested class for device reference, similar to LogRecord
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DeviceReference {
        public String id;
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public OffsetDateTime getDateTime() { return dateTime; }
    public void setDateTime(OffsetDateTime dateTime) { this.dateTime = dateTime; }
    public Double getData() { return data; }
    public void setData(Double data) { this.data = data; }
    public DeviceReference getDevice() { return device; }
    public void setDevice(DeviceReference device) { this.device = device; }
    public Diagnostic getDiagnostic() { return diagnostic; }
    public void setDiagnostic(Diagnostic diagnostic) { this.diagnostic = diagnostic; }

    @Override
    public String toString() {
        return "StatusData{" +
                "id='" + id + '\'' +
                ", dateTime=" + dateTime +
                ", data=" + data +
                ", deviceId='" + (device != null ? device.getId() : "null") + '\'' +
                ", diagnosticId='" + (diagnostic != null ? diagnostic.getId() : "null") + '\'' +
                '}';
    }
}
