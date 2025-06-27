package com.jiss.example.geotab;

import com.jiss.example.geotab.model.Device;
import com.jiss.example.geotab.model.GeotabApiRequest;
import com.jiss.example.geotab.model.LoginResponse;
import com.jiss.example.geotab.model.LoginResult;
import com.jiss.example.geotab.model.LogRecord;
import com.jiss.example.geotab.model.FeedResult;
import com.jiss.example.geotab.model.StatusData;
import com.jiss.example.geotab.model.EnrichedLogRecord;
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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main service for interacting with the Geotab API and managing vehicle data backup.
 * This class handles Phase 1: Initial Vehicle Discovery, including authentication.
 */
@ApplicationScoped
public class GeotabDataService {


    private static final String LOG_RECORD_FEED_TYPE = "LogRecord";
    private static final String STATUS_DATA_FEED_TYPE = "StatusData";
    private static final String DIAGNOSTIC_ODOMETER_ID = "DiagnosticOdometerId";
    private static final String VERSIONS_FILE_NAME = "last_processed_versions.json";
    private static final DateTimeFormatter ISO_Z_FORMATTER = DateTimeFormatter.ISO_INSTANT; // For Z-ending timestamps

    // Define a threshold for matching odometer readings (e.g., within 5 seconds)
    private static final Duration ODOMETER_MATCH_THRESHOLD = Duration.ofSeconds(10);



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


    private String sessionId;

    // We'll manage discovered devices here
    private final Map<String, Device> discoveredVehicles = new ConcurrentHashMap<>();

    // This map will store the last processed version for each feed type per vehicle
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
    void backupVehicleData() {
        System.out.println("\n--- Starting scheduled LogRecord backup (" + OffsetDateTime.now() + ") ---");
        if (discoveredVehicles.isEmpty()) {
            System.out.println("No vehicles discovered. Skipping LogRecord backup.");
            return;
        }

        try {
            for (Device device : discoveredVehicles.values()) {
                System.out.println("\nProcessing data for Device ID: " + device.getId() + " (" + device.getName() + ")");

                // 1. Fetch new LogRecords
                List<LogRecord> newLogRecords = processLogRecordsForVehicle(device);

                // 2. Fetch new StatusData (Odometer)
                List<StatusData> newOdometerStatusData = processStatusDataForVehicle(device);

                // 3. Merge and append to final CSV
                if (!newLogRecords.isEmpty()) {
                    List<EnrichedLogRecord> enrichedRecords = matchLogRecordsWithOdometer(device, newLogRecords, newOdometerStatusData);
                    appendEnrichedRecordsToCsv(device, enrichedRecords);
                } else {
                    System.out.println("  No new LogRecords to enrich for Device ID: " + device.getId());
                }
                //processLogRecordsForVehicle(device);
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
     *
     * @param device The Device object to process.
     * @return
     * @throws IOException If an API or file operation error occurs.
     */
    private List<LogRecord> processLogRecordsForVehicle(Device device) throws IOException {
        System.out.println("Processing LogRecords for Device ID: " + device.getId() + " (" + device.getName() + ")");

        List<LogRecord> newLogRecords = Collections.emptyList();

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

        params.setCredentials(getAuthenticatedCredentials());

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

            newLogRecords = feedResult.getData();
            String newToVersion = feedResult.getToVersion();

            if (newLogRecords != null && !newLogRecords.isEmpty()) {
                System.out.println("  Retrieved " + newLogRecords.size() + " new LogRecords.");
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

        return newLogRecords;
    }

    /**
     * Fetches new StatusData (Odometer) for a single vehicle using GetFeed.
     * @param device The Device object to process.
     * @return List of newly retrieved StatusData objects filtered for Odometer.
     * @throws IOException If an API error occurs.
     */
    private List<StatusData> processStatusDataForVehicle(Device device) throws IOException {
        System.out.println("  Fetching Odometer StatusData...");

        List<StatusData> newOdometerStatusData = Collections.emptyList();

        GeotabApiRequest.Params params = new GeotabApiRequest.Params();
        params.setTypeName(LOG_RECORD_FEED_TYPE);

        String fromVersion = lastProcessedVersions.computeIfAbsent(device.getId(), k -> new ConcurrentHashMap<>())
                .get(STATUS_DATA_FEED_TYPE);

        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("deviceSearch", Map.of("id", device.getId()));
        searchParams.put("diagnosticSearch", Map.of("id", DIAGNOSTIC_ODOMETER_ID)); // Filter for Odometer data

        if (fromVersion != null) {
            params.setFromVersion(fromVersion);
            System.out.println("    Odometer: Using fromVersion: " + fromVersion);
        } else {
            OffsetDateTime twentyFourHoursAgo = OffsetDateTime.now(ZoneOffset.UTC).minusHours(24); // Adjust as needed
            searchParams.put("fromDate", twentyFourHoursAgo.format(ISO_Z_FORMATTER));
            System.out.println("    Odometer: No existing version. Seeding fromDate: " + twentyFourHoursAgo);
        }
        params.setSearch(searchParams);

        // Max results per call, important for large data sets
        params.setResultsLimit(50000); // Max for LogRecord is 50,000

        params.setCredentials(getAuthenticatedCredentials());

        GeotabApiRequest request = new GeotabApiRequest("GetFeed", params);
        JsonNode responseNode = geotabApiClient.call(objectMapper.convertValue(request, Map.class));

        if (responseNode.has("result")) {
            TypeFactory typeFactory = objectMapper.getTypeFactory();
            FeedResult<StatusData> feedResult = objectMapper.readValue(
                    responseNode.get("result").traverse(),
                    typeFactory.constructParametricType(FeedResult.class, StatusData.class)
            );

            // Filter data to ensure it's specifically Odometer (in case API returns other types despite diagnosticSearch)
            newOdometerStatusData = feedResult.getData() != null ?
                    feedResult.getData().stream()
                            .filter(sd -> sd.getDiagnostic() != null && DIAGNOSTIC_ODOMETER_ID.equals(sd.getDiagnostic().getId()))
                            .collect(Collectors.toList()) :
                    Collections.emptyList();
            String newToVersion = feedResult.getToVersion();

            if (!newOdometerStatusData.isEmpty()) {
                System.out.println("    Retrieved " + newOdometerStatusData.size() + " new Odometer StatusData.");
            } else {
                System.out.println("    No new Odometer StatusData found.");
            }

            // Always update toVersion
            if (newToVersion != null) {
                lastProcessedVersions.computeIfAbsent(device.getId(), k -> new ConcurrentHashMap<>())
                        .put(STATUS_DATA_FEED_TYPE, newToVersion);
            }

        } else if (responseNode.has("error")) {
            System.err.println("    Error fetching Odometer StatusData for device " + device.getId() + ": " + responseNode.get("error").toString());
        } else {
            System.err.println("    Unexpected API response for Odometer StatusData for device " + device.getId() + ": " + responseNode.toString());
        }

        return newOdometerStatusData;
    }

    /**
     * Matches LogRecords with the closest Odometer StatusData by timestamp.
     * Prefers odometer readings that occur at or before the log record's timestamp.
     *
     * @param device The device being processed.
     * @param logRecords The list of LogRecords to enrich.
     * @param odometerData The list of available Odometer StatusData for matching.
     * @return A list of EnrichedLogRecord objects.
     */
    private List<EnrichedLogRecord> matchLogRecordsWithOdometer(Device device, List<LogRecord> logRecords, List<StatusData> odometerData) {
        System.out.println("  Matching LogRecords with Odometer data...");

        List<EnrichedLogRecord> enrichedRecords = new ArrayList<>();
        String vin = device.getVehicleIdentificationNumber();

        // Sort odometer data by timestamp for efficient searching
        // This is crucial for finding the "closest previous" odometer reading
        odometerData.sort(Comparator.comparing(StatusData::getDateTime));

        for (LogRecord log : logRecords) {
            Double matchedOdometerValue = null;
            StatusData closestOdometer = null;
            Duration minDiff = ODOMETER_MATCH_THRESHOLD; // Max allowed difference

            // Iterate through sorted odometer data to find the closest value
            for (StatusData odometer : odometerData) {
                // Only consider odometer readings that are at or before the log record's timestamp
                if (odometer.getDateTime().isAfter(log.getDateTime())) {
                    break; // Since data is sorted, no need to check further
                }

                Duration diff = Duration.between(odometer.getDateTime(), log.getDateTime()).abs();

                // Find the closest odometer reading *before or at* the log record's timestamp
                // If multiple are equally close, the last one found (closest to log.dateTime) is chosen
                if (diff.compareTo(minDiff) < 0) {
                    closestOdometer = odometer;
                    minDiff = diff;
                }
            }

            if (closestOdometer != null) {
                matchedOdometerValue = closestOdometer.getData();
            }

            // Create the enriched record
            enrichedRecords.add(new EnrichedLogRecord(
                    device.getId(),
                    vin,
                    log.getDateTime(),
                    log.getLatitude(),
                    log.getLongitude(),
                    matchedOdometerValue
            ));
        }
        System.out.println("  Matched " + enrichedRecords.size() + " LogRecords with Odometer data.");
        return enrichedRecords;
    }

    /**
     * Appends new EnrichedLogRecord data to the vehicle's final CSV file.
     *
     * @param device The device the records belong to.
     * @param enrichedRecords The list of new EnrichedLogRecord objects to append.
     * @throws IOException If there's an error writing to the CSV file.
     */
    private void appendEnrichedRecordsToCsv(Device device, List<EnrichedLogRecord> enrichedRecords) throws IOException {
        String filename = device.getId() + "_enriched_data.csv"; // Final merged CSV
        Path filePath = Paths.get(outputDirectory, filename);

        // Ensure the output directory exists
        Files.createDirectories(filePath.getParent());

        boolean fileExists = Files.exists(filePath);
        boolean fileIsEmpty = !fileExists || Files.size(filePath) == 0;

        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath.toFile(), true))) { // 'true' for append mode
            if (fileIsEmpty) {
                // Write header only if the file is new or empty
                writer.writeNext(new String[]{"Vehicle ID", "VIN", "Timestamp", "Latitude", "Longitude", "OdometerValue"});
            }

            for (EnrichedLogRecord record : enrichedRecords) {
                writer.writeNext(record.toCsvRow());
            }
            System.out.println("  Appended " + enrichedRecords.size() + " enriched records to " + filename);
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
}
