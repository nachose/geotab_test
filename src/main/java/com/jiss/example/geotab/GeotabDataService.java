package com.jiss.example.geotab;

import com.jiss.example.geotab.model.Device;
import com.jiss.example.geotab.model.GeotabApiRequest;
import com.jiss.example.geotab.model.LoginResponse;
import com.jiss.example.geotab.model.LoginResult;
import com.jiss.example.geotab.model.LogRecord;
import com.jiss.example.geotab.model.FeedResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import com.opencsv.CSVWriter;

import jakarta.enterprise.context.ApplicationScoped;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Main service for interacting with the Geotab API and managing vehicle data backup.
 * This class handles Phase 1: Initial Vehicle Discovery, including authentication.
 */
@ApplicationScoped
public class GeotabDataService {

    private static final String LOG_RECORD_FEED_TYPE = "LogRecord";
    private static final String VERSIONS_FILE_NAME = "last_processed_versions.json";
    private static final DateTimeFormatter ISO_Z_FORMATTER = DateTimeFormatter.ISO_INSTANT; // For Z-ending timestamps


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


    // --- NEW: Field to store the session ID ---
    private String sessionId;

    // We'll manage discovered devices here (Phase 1)
    private final Map<String, Device> discoveredVehicles = new ConcurrentHashMap<>();

    // This map will store the last processed version for each feed type per vehicle (Phase 2)
    // Key: vehicleId, Value: Map<FeedTypeName, LastVersionString>
    private final Map<String, Map<String, String>> lastProcessedVersions = new ConcurrentHashMap<>();

    // ObjectMapper for JSON serialization/deserialization.
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    /**
     * Utility method to construct the credentials map specifically for the
     * 'Authenticate' API call.
     */
    private Map<String, Object> getLoginCredentialsForAuthenticate() {
        Map<String, Object> credentials = new HashMap<>();
        credentials.put("database", geotabDatabase);
        credentials.put("userName", geotabUsername);
        credentials.put("password", geotabPassword);
        return credentials;
    }

    /**
     * Utility method to construct the credentials map for subsequent API calls
     * after a successful authentication.
     */
    private LoginResult.Credentials getAuthenticatedCredentials() {
        if (sessionId == null) {
            // This should not happen if login() is called successfully beforehand
            throw new IllegalStateException("Session ID is null. Must log in first before making authenticated calls.");
        }
        LoginResult.Credentials credentials = new LoginResult.Credentials();
        credentials.setSessionId(sessionId);
        credentials.setDatabase(geotabDatabase); // Geotab often uses 'path' for database name post-login
        credentials.setUserName(geotabUsername);
        return credentials;
    }

    /**
     * Performs the authentication against the Geotab API to obtain a session ID.
     * This must be called before any other API methods like Get or GetFeed.
     *
     * @throws IOException If the login fails due to API error or unexpected response.
     */
    private void login() throws IOException {
        System.out.println("Attempting to login to Geotab API...");

        // For the 'Authenticate' method, the credentials are directly in the 'params' of the JSON-RPC request.
        // We manually construct the request map here to be precise.
        Map<String, Object> authRequestBody = new HashMap<>();
        authRequestBody.put("jsonrpc", "2.0");
        authRequestBody.put("method", "Authenticate");
        authRequestBody.put("params", getLoginCredentialsForAuthenticate()); // Login credentials go here
        authRequestBody.put("id", 1); // Request ID

        JsonNode responseNode = geotabApiClient.call(authRequestBody);

        if (responseNode.has("result")) {
            LoginResponse loginResponse = objectMapper.treeToValue(responseNode, LoginResponse.class);
            System.out.println("Login response: " + loginResponse);
            LoginResult loginResult = loginResponse.getResult();
            System.out.println("Login result: " + loginResult);
            this.sessionId = loginResult.getSessionId();
            System.out.println("Successfully logged in to Geotab API. Session ID obtained. : " + sessionId);
        } else if (responseNode.has("error")) {
            throw new IOException("Geotab API error during login: " + responseNode.get("error").toString());
        } else {
            throw new IOException("Unexpected Geotab API response during login: " + responseNode.toString());
        }
    }

    // --- Version Persistence Methods ---
    private Path getVersionsFilePath() {
        return Paths.get(outputDirectory, VERSIONS_FILE_NAME);
    }

    @SuppressWarnings("unchecked")
    private void loadLastProcessedVersions() {
        Path versionsPath = getVersionsFilePath();
        if (Files.exists(versionsPath)) {
            try {
                String json = Files.readString(versionsPath);
                // The stored structure is Map<String, Map<String, String>>
                TypeFactory typeFactory = objectMapper.getTypeFactory();
                // Construct JavaType for the key (String)
                JavaType keyType = typeFactory.constructType(String.class);
                // Construct the inner Map<String, String> type
                JavaType innerMapType = typeFactory.constructMapType(HashMap.class, String.class, String.class);
                // Construct the outer Map<String, Map<String, String>> type
                MapType mapType = typeFactory.constructMapType(HashMap.class, keyType, innerMapType);
                Map<String, Map<String, String>> loadedMap = objectMapper.readValue(json, mapType);
                lastProcessedVersions.clear();
                lastProcessedVersions.putAll(loadedMap);
                System.out.println("Loaded last processed versions from " + VERSIONS_FILE_NAME);
            } catch (IOException e) {
                System.err.println("Error loading last processed versions: " + e.getMessage());
                // Continue with empty map if load fails
            }
        } else {
            System.out.println("No existing versions file found. Starting fresh.");
        }
    }

    private void saveLastProcessedVersions() {
        Path versionsPath = getVersionsFilePath();
        try {
            Files.createDirectories(versionsPath.getParent()); // Ensure parent directory exists
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(lastProcessedVersions);
            Files.writeString(versionsPath, json);
            System.out.println("Saved last processed versions to " + VERSIONS_FILE_NAME);
        } catch (IOException e) {
            System.err.println("Error saving last processed versions: " + e.getMessage());
        }
    }


    /**
     * This method is automatically called by Quarkus once the application has started.
     * It orchestrates the initial login and then vehicle discovery.
     */
    // --- Startup and Main Scheduled Task ---
    @Startup
    void onStart() {
        System.out.println("Application starting up: Initiating Phase 1 - Authentication and Vehicle Discovery...");
        try {
            login(); // Step 1: Authenticate
            discoverAllVehicles(); // Step 2: Discover vehicles
            loadLastProcessedVersions(); // Step 3: Load persistence state

            System.out.println("Phase 1: Initialization complete. Found " + discoveredVehicles.size() + " vehicles.");
            if (!discoveredVehicles.isEmpty()) {
                System.out.println("Discovered Vehicles:");
                discoveredVehicles.values().forEach(System.out::println);
            } else {
                System.out.println("No vehicles found with provided credentials/database.");
            }
        } catch (IOException e) {
            System.err.println("CRITICAL ERROR: Failed to initialize application (authentication or discovery failed). " + e.getMessage());
            e.printStackTrace();
            // Depending on criticality, you might want to exit the application or implement a retry mechanism.
        }
    }

    /**
     * Phase 2A: Scheduled task to incrementally backup LogRecord data for each vehicle.
     * Runs every minute based on 'geotab.backup.schedule' cron expression.
     */
    @Scheduled(cron = "{geotab.backup.schedule}")
    void backupLogRecords() {
        System.out.println("\n--- Starting scheduled LogRecord backup (" + OffsetDateTime.now() + ") ---");
        if (discoveredVehicles.isEmpty()) {
            System.out.println("No vehicles discovered. Skipping LogRecord backup.");
            return;
        }

        try {
            for (Device device : discoveredVehicles.values()) {
                processLogRecordsForVehicle(device);
            }
            saveLastProcessedVersions(); // Save state after processing all vehicles
            System.out.println("--- LogRecord backup completed ---");
        } catch (Exception e) {
            System.err.println("Error during scheduled LogRecord backup: " + e.getMessage());
            e.printStackTrace();
            // This ensures the scheduler continues to run even if one iteration fails.
        }
    }

    /**
     * Processes LogRecord data for a single vehicle using GetFeed.
     * @param device The Device object to process.
     * @throws IOException If an API or file operation error occurs.
     */
    private void processLogRecordsForVehicle(Device device) throws IOException {
        System.out.println("Processing LogRecords for Device ID: " + device.getId() + " (" + device.getName() + ")");

        GeotabApiRequest.Params params = new GeotabApiRequest.Params();
        params.setTypeName(LOG_RECORD_FEED_TYPE);

        // Get the last processed version for this device's LogRecord feed
        String fromVersion = lastProcessedVersions.computeIfAbsent(device.getId(), k -> new ConcurrentHashMap<>())
                .get(LOG_RECORD_FEED_TYPE);

        // Define search parameters
        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("deviceSearch", Map.of("id", device.getId()));

        if (fromVersion != null) {
            // If we have a version, use it for incremental updates
            params.setFromVersion(fromVersion);
            System.out.println("  Using fromVersion: " + fromVersion);
        } else {
            // If no version (first run for this device/feed), use fromDate to seed data
            // Default to getting data from the last 24 hours (adjust as needed for demo data)
            OffsetDateTime twentyFourHoursAgo = OffsetDateTime.now(ZoneOffset.UTC).minusHours(24);
            searchParams.put("fromDate", twentyFourHoursAgo.format(ISO_Z_FORMATTER));
            System.out.println("  No existing version. Seeding fromDate: " + twentyFourHoursAgo);
        }
        params.setSearch(searchParams);

        // Max results per call, important for large data sets
        params.setResultsLimit(50000); // Max for LogRecord is 50,000

        // Create the API request
        GeotabApiRequest request = new GeotabApiRequest("GetFeed", params);

        // Make the API call
        JsonNode responseNode = geotabApiClient.call(objectMapper.convertValue(request, Map.class));

        if (responseNode.has("result")) {
            // Need to specify the generic type for FeedResult for Jackson
            TypeFactory typeFactory = objectMapper.getTypeFactory();
            FeedResult<LogRecord> feedResult = objectMapper.readValue(
                    responseNode.get("result").traverse(),
                    typeFactory.constructParametricType(FeedResult.class, LogRecord.class)
            );

            List<LogRecord> newLogRecords = feedResult.getData();
            String newToVersion = feedResult.getToVersion();

            if (newLogRecords != null && !newLogRecords.isEmpty()) {
                System.out.println("  Retrieved " + newLogRecords.size() + " new LogRecords.");
                appendLogRecordsToCsv(device, newLogRecords);
                // Update the last processed version
                lastProcessedVersions.computeIfAbsent(device.getId(), k -> new ConcurrentHashMap<>())
                        .put(LOG_RECORD_FEED_TYPE, newToVersion);
            } else {
                System.out.println("  No new LogRecords found.");
            }
            // Even if no new data, update toVersion to current state to avoid re-fetching old empty range
            // This is crucial for GetFeed to advance correctly even with no new records.
            if (newToVersion != null) {
                lastProcessedVersions.computeIfAbsent(device.getId(), k -> new ConcurrentHashMap<>())
                        .put(LOG_RECORD_FEED_TYPE, newToVersion);
            }


        } else if (responseNode.has("error")) {
            System.err.println("  Error fetching LogRecords for device " + device.getId() + ": " + responseNode.get("error").toString());
        } else {
            System.err.println("  Unexpected API response for device " + device.getId() + ": " + responseNode.toString());
        }
    }

    /**
     * Appends new LogRecord data to the vehicle's CSV file.
     * The CSV format for this phase is: Vehicle ID, Timestamp, Latitude, Longitude.
     * VIN and Odometer will be added in a later merging phase.
     *
     * @param device The device the log records belong to.
     * @param logRecords The list of new LogRecord objects to append.
     * @throws IOException If there's an error writing to the CSV file.
     */
    private void appendLogRecordsToCsv(Device device, List<LogRecord> logRecords) throws IOException {
        String filename = device.getId() + "_log_records.csv"; // CSV for LogRecords only
        Path filePath = Paths.get(outputDirectory, filename);

        // Ensure the output directory exists
        Files.createDirectories(filePath.getParent());

        boolean fileExists = Files.exists(filePath);

        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath.toFile(), true))) { // 'true' for append mode
            if (!fileExists || Files.size(filePath) == 0) {
                // Write header only if the file is new or empty
                writer.writeNext(new String[]{"Vehicle ID", "Timestamp", "Latitude", "Longitude"});
            }

            for (LogRecord record : logRecords) {
                // Prepare data for CSV row
                String vehicleId = device.getId(); // Use the primary device ID
                String timestamp = record.getDateTime().format(ISO_Z_FORMATTER); // ISO-8601 format with Z for UTC
                String latitude = String.valueOf(record.getLatitude());
                String longitude = String.valueOf(record.getLongitude());

                writer.writeNext(new String[]{vehicleId, timestamp, latitude, longitude});
            }
            System.out.println("  Appended " + logRecords.size() + " LogRecords to " + filename);
        }
    }

    // --- Getter for discovered vehicles (for testing/inspection) ---
    public Map<String, Device> getDiscoveredVehicles() {
        return Collections.unmodifiableMap(discoveredVehicles); // Return unmodifiable map
    }

    // --- Helper methods for lastProcessedVersions (might be useful for tests) ---
    public String getLastVersion(String deviceId, String feedTypeName) {
        return lastProcessedVersions.computeIfAbsent(deviceId, k -> new ConcurrentHashMap<>())
                .get(feedTypeName);
    }

    public void setLastVersion(String deviceId, String feedTypeName, String version) {
        lastProcessedVersions.computeIfAbsent(deviceId, k -> new ConcurrentHashMap<>())
                .put(feedTypeName, version);
    }

    /**
     * Performs the initial discovery of all available vehicles using the Geotab 'Get' API method,
     * utilizing the authenticated session. Populates the 'discoveredVehicles' map.
     *
     * @throws IOException If there's an error during API communication or response parsing.
     */
    public void discoverAllVehicles() throws IOException {
        System.out.println("Making Geotab API 'Get' call for 'Device' type using authenticated session...");

        GeotabApiRequest.Params params = new GeotabApiRequest.Params();
        params.setTypeName("Device"); // Specify the type of object we  want to retrieve
        params.setCredentials(getAuthenticatedCredentials()); // Use the authenticated credentials

        GeotabApiRequest request = new GeotabApiRequest("Get", params);

        // Convert the request object to a Map for the RestClient and make the API call
        JsonNode responseNode = geotabApiClient.call(objectMapper.convertValue(request, Map.class));

        // Check for 'result' field in the response (success)
        if (responseNode.has("result") && responseNode.get("result").isArray()) {
            List<Device> devices = objectMapper.readerForListOf(Device.class).readValue(responseNode.get("result"));

            // Clear any previous discoveries and populate the map with new devices
            discoveredVehicles.clear();
            devices.forEach(device -> discoveredVehicles.put(device.getId(), device));

            System.out.println("Successfully retrieved and stored " + devices.size() + " devices from Geotab API.");
        }
        // Check for 'error' field in the response (API-level error)
        else if (responseNode.has("error")) {
            throw new IOException("Geotab API error during device discovery: " + responseNode.get("error").toString());
        }
        // Handle unexpected response structures
        else {
            throw new IOException("Unexpected Geotab API response during device discovery: " + responseNode.toString());
        }
    }

    /**
     * Getter for the map of discovered vehicles.
     * @return A map of Device ID to Device object.
     */
//    public Map<String, Device> getDiscoveredVehicles() {
//        return discoveredVehicles;
//    }

    /**
     * Helper method to retrieve the last processed version for a specific feed type
     * and device. This will be used in Phase 2 for incremental updates.
     * @param deviceId The ID of the device.
     * @param feedTypeName The type of feed (e.g., "LogRecord", "StatusData").
     * @return The last version string, or null if not found.
     */
//    public String getLastVersion(String deviceId, String feedTypeName) {
//        return lastProcessedVersions.computeIfAbsent(deviceId, k -> new ConcurrentHashMap<>())
//                .get(feedTypeName);
//    }

    /**
     * Helper method to set (store) the last processed version for a specific feed type
     * and device. This will be used in Phase 2 for incremental updates.
     * @param deviceId The ID of the device.
     * @param feedTypeName The type of feed.
     * @param version The new version string to store.
     */
//    public void setLastVersion(String deviceId, String feedTypeName, String version) {
//        lastProcessedVersions.computeIfAbsent(deviceId, k -> new ConcurrentHashMap<>())
//                .put(feedTypeName, version);
//    }

    // IMPORTANT NOTE: The 'lastProcessedVersions' map currently only stores state in memory.
    // For a robust, containerized solution, this state MUST be persisted to disk (e.g., a JSON file
    // in the mounted volume or a lightweight embedded database) so that it is not lost when
    // the container restarts. This persistence logic will be crucial for Phase 2.
}
