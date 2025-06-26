package com.jiss.example.geotab;


import com.jiss.example.geotab.model.Device;
import com.jiss.example.geotab.model.LogRecord;
import com.jiss.example.geotab.model.GeotabApiRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// Using ConcurrentHashMap to store the last processed date for each vehicle
// This ensures thread-safety if multiple scheduled tasks were ever to overlap (though not expected here)
@ApplicationScoped
@Startup // Initialize on application start
public class GeotabDataService {

    @RestClient
    GeotabApiClient geotabApiClient;

    @ConfigProperty(name = "geotab.api.database")
    String geotabDatabase;

    @ConfigProperty(name = "geotab.api.username")
    String geotabUsername;

    @ConfigProperty(name = "geotab.api.password")
    String geotabPassword;

    @ConfigProperty(name = "geotab.backup.output.directory")
    String outputDirectory;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final Map<String, OffsetDateTime> lastProcessedTimestamps = new ConcurrentHashMap<>();
    private String geotabSessionId = null; // To store the session ID after login

    // Call this method once to log in and get a session ID (if required by the API)
    // Geotab's API often takes credentials with each call, but a session ID might optimize
    private Map<String, Object> getCredentials() {
        Map<String, Object> credentials = new HashMap<>();
        credentials.put("database", geotabDatabase);
        credentials.put("userName", geotabUsername);
        credentials.put("password", geotabPassword);
        // If a session ID is managed by Geotab, you might need to add it here
         if (geotabSessionId != null) {
             credentials.put("sessionId", geotabSessionId);
         }
        return credentials;
    }

    // Scheduled task to run every minute
    @Scheduled(cron = "{geotab.backup.schedule}")
    void backupVehicleData() {
        System.out.println("Starting scheduled Geotab data backup...");
        try {
            // Step 1: Get all vehicles
            List<Device> vehicles = getAllVehicles();
            if (vehicles.isEmpty()) {
                System.out.println("No vehicles found.");
                return;
            }

            // Step 2: For each vehicle, retrieve and append new LogRecord data
            for (Device vehicle : vehicles) {
                processVehicleData(vehicle);
            }

            System.out.println("Geotab data backup completed.");

        } catch (Exception e) {
            System.err.println("Error during Geotab data backup: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private List<Device> getAllVehicles() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("typeName", "Device");
        params.put("search", Map.of("groups", List.of(Map.of("id", "GroupVehicle")))); // Assuming "GroupVehicle" is the ID for all vehicles
        // FromDate for active vehicles:
        params.put("search", Map.of("fromDate", OffsetDateTime.now(ZoneOffset.UTC).minusYears(5).format(DateTimeFormatter.ISO_INSTANT))); // Adjust as needed to get all vehicles, or omit if not required to filter by date

        GeotabApiRequest request = new GeotabApiRequest("Get", params, getCredentials());
        String responseJson = geotabApiClient.call(objectMapper.convertValue(request, Map.class));
        JsonNode rootNode = objectMapper.readTree(responseJson);

        if (rootNode.has("result")) {
            return objectMapper.readerForListOf(Device.class).readValue(rootNode.get("result"));
        }
        throw new IOException("Failed to get vehicles: " + responseJson);
    }

    private void processVehicleData(Device vehicle) throws IOException {
        String vehicleId = vehicle.id;
        String vin = vehicle.vehicleIdentificationNumber != null ? vehicle.vehicleIdentificationNumber : "UNKNOWN_VIN";
        String vehicleName = vehicle.name != null ? vehicle.name : "UNKNOWN_VEHICLE";

        System.out.printf("Processing data for vehicle: %s (VIN: %s)%n", vehicleName, vin);

        // Get the last processed timestamp for this vehicle
        OffsetDateTime fromDate = lastProcessedTimestamps.getOrDefault(vehicleId, OffsetDateTime.MIN); // Start from min if no previous data

        // Use Geotab's GetFeed for incremental updates
        Map<String, Object> params = new HashMap<>();
        params.put("typeName", "LogRecord");
        params.put("search", Map.of("deviceSearch", Map.of("id", vehicleId)));
        // Geotab's GetFeed uses a 'fromVersion' or 'fromVersionId' for incremental,
        // or a 'fromDate' when starting a feed. We'll use 'fromDate' as a simple start.
        // For true incremental, you'd save the last 'version' from the GetFeed response.
        if (!fromDate.equals(OffsetDateTime.MIN)) {
            params.put("search", Map.of("deviceSearch", Map.of("id", vehicleId), "fromDate", fromDate.format(DateTimeFormatter.ISO_INSTANT)));
        }

        GeotabApiRequest request = new GeotabApiRequest("GetFeed", params, getCredentials());
        String responseJson = geotabApiClient.call(objectMapper.convertValue(request, Map.class));
        JsonNode rootNode = objectMapper.readTree(responseJson);

        if (rootNode.has("result") && rootNode.get("result").has("data")) {
            List<LogRecord> newLogRecords = objectMapper.readerForListOf(LogRecord.class).readValue(rootNode.get("result").get("data"));
            String lastVersion = rootNode.get("result").has("toVersion") ? rootNode.get("result").get("toVersion").asText() : null; // This is crucial for true incremental

            if (!newLogRecords.isEmpty()) {
                System.out.printf("Found %d new log records for vehicle %s%n", newLogRecords.size(), vehicleName);
                appendToCsv(vehicleId, vin, newLogRecords);

                // Update the last processed timestamp (or version)
                // For simplicity, we'll use the dateTime of the last record.
                // For robustness with GetFeed, you'd store and use the 'toVersion' from the API response.
                newLogRecords.stream()
                        .map(LogRecord::getDateTime)
                        .max(OffsetDateTime::compareTo)
                        .ifPresent(latestDateTime -> lastProcessedTimestamps.put(vehicleId, latestDateTime));
            } else {
                System.out.printf("No new log records for vehicle %s%n", vehicleName);
            }
        } else {
            System.err.printf("Failed to get log records for vehicle %s: %s%n", vehicleName, responseJson);
        }
    }

    private void appendToCsv(String vehicleId, String vin, List<LogRecord> records) throws IOException {
        Path outputPath = Paths.get(outputDirectory, vehicleId + ".csv");
        Files.createDirectories(outputPath.getParent());

        boolean fileExists = Files.exists(outputPath);
        try (FileWriter fw = new FileWriter(outputPath.toFile(), true);
             com.opencsv.CSVWriter writer = new com.opencsv.CSVWriter(fw)) {

            // Write header only if the file is new
            if (!fileExists || Files.size(outputPath) == 0) {
                writer.writeNext(new String[]{"Vehicle ID", "Timestamp", "VIN", "Latitude", "Longitude", "Odometer"});
            }

            for (LogRecord record : records) {
                // Ensure all fields are available before writing
                if (record.hasAllRequiredFields()) {
                    writer.writeNext(new String[]{
                            vehicleId,
                            record.getDateTime().format(DateTimeFormatter.ISO_INSTANT),
                            vin,
                            String.valueOf(record.getLatitude()),
                            String.valueOf(record.getLongitude()),
                            String.valueOf(record.getOdometer())
                    });
                } else {
                    System.err.printf("Skipping incomplete log record for vehicle %s: %s%n", vehicleId, record);
                }
            }
        }
    }
}
