package com.jiss.example.geotab;

import com.jiss.example.geotab.model.Device;
import com.jiss.example.geotab.model.GeotabApiRequest;
import com.jiss.example.geotab.model.LoginResponse;
import com.jiss.example.geotab.model.LoginResult;
import com.jiss.example.geotab.model.LogRecord;
import com.jiss.example.geotab.model.FeedResult;
import com.jiss.example.geotab.model.StatusData;
import com.jiss.example.geotab.model.EnrichedLogRecord;
import com.jiss.example.geotab.model.MultiCallItem;
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
import org.jboss.logging.Logger;

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
import java.util.concurrent.atomic.AtomicInteger;
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

    private static final Logger LOG = Logger.getLogger(GeotabDataService.class);


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
        LOG.info("Attempting to login to Geotab API...");

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
            LOG.info("Login response: " + loginResponse);
            LoginResult loginResult = loginResponse.getResult();
            LOG.info("Login result: " + loginResult);
            this.sessionId = loginResult.getSessionId();
            LOG.info("Successfully logged in to Geotab API. Session ID obtained. : " + sessionId);
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
                LOG.info("Loaded last processed versions from " + VERSIONS_FILE_NAME);
            } catch (IOException e) {
                LOG.info("Error loading last processed versions: " + e.getMessage());
                // Continue with empty map if load fails
            }
        } else {
            LOG.info("No existing versions file found. Starting fresh.");
        }
    }

    private void saveLastProcessedVersions() {
        Path versionsPath = getVersionsFilePath();
        try {
            Files.createDirectories(versionsPath.getParent()); // Ensure parent directory exists
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(lastProcessedVersions);
            Files.writeString(versionsPath, json);
            LOG.info("Saved last processed versions to " + VERSIONS_FILE_NAME);
        } catch (IOException e) {
            LOG.info("Error saving last processed versions: " + e.getMessage());
        }
    }


    /**
     * This method is automatically called by Quarkus once the application has started.
     * It orchestrates the initial login and then vehicle discovery.
     */
    // --- Startup and Main Scheduled Task ---
    @Startup
    void onStart() {
        LOG.info("Application starting up: Initiating Phase 1 - Authentication and Vehicle Discovery...");
        try {
            login(); // Step 1: Authenticate
            discoverAllVehicles(); // Step 2: Discover vehicles
            loadLastProcessedVersions(); // Step 3: Load persistence state

            LOG.info("Phase 1: Initialization complete. Found " + discoveredVehicles.size() + " vehicles.");
            if (!discoveredVehicles.isEmpty()) {
                LOG.info("Discovered Vehicles:");
                discoveredVehicles.values().forEach(System.out::println);
            } else {
                LOG.info("No vehicles found with provided credentials/database.");
            }
        } catch (IOException e) {
            LOG.info("CRITICAL ERROR: Failed to initialize application (authentication or discovery failed). " + e.getMessage());
            e.printStackTrace();
            // Depending on criticality, you might want to exit the application or implement a retry mechanism.
        }
    }


    /**
     * Phase 2A & 2B: Scheduled task to incrementally backup LogRecord and StatusData (Odometer)
     * for all vehicles using a single ExecuteMultiCall request, and then merge the data.
     */
    @Scheduled(cron = "{geotab.backup.schedule}")
    void backupVehicleData() {
        LOG.info("\n--- Starting scheduled vehicle data backup (" + OffsetDateTime.now() + ") ---");
        if (discoveredVehicles.isEmpty()) {
            LOG.info("No vehicles discovered. Skipping data backup.");
            return;
        }

        try {
            // Map to store requests for matching responses later: id -> {deviceId, feedType}
            Map<Integer, Map.Entry<String, String>> requestMetadata = new ConcurrentHashMap<>();
            List<MultiCallItem> multiCalls = new ArrayList<>();
            AtomicInteger requestIdCounter = new AtomicInteger(1); // Unique ID for each sub-call

            LOG.info("Preparing multi-call requests for " + discoveredVehicles.size() + " devices...");

            for (Device device : discoveredVehicles.values()) {
                // Prepare LogRecord GetFeed call
                int logRecordRequestId = requestIdCounter.getAndIncrement();
                GeotabApiRequest.Params logRecordParams = createGetFeedParams(device, LOG_RECORD_FEED_TYPE);
                multiCalls.add(new MultiCallItem("GetFeed", logRecordParams, logRecordRequestId));
                requestMetadata.put(logRecordRequestId, Map.entry(device.getId(), LOG_RECORD_FEED_TYPE));

                // Prepare StatusData (Odometer) GetFeed call
                int odometerRequestId = requestIdCounter.getAndIncrement();
                GeotabApiRequest.Params odometerParams = createGetFeedParams(device, STATUS_DATA_FEED_TYPE, DIAGNOSTIC_ODOMETER_ID);
                multiCalls.add(new MultiCallItem("GetFeed", odometerParams, odometerRequestId));
                requestMetadata.put(odometerRequestId, Map.entry(device.getId(), STATUS_DATA_FEED_TYPE));
            }

            // Construct the ExecuteMultiCall master request
            Map<String, Object> multiCallRequestBody = new HashMap<>();
            multiCallRequestBody.put("jsonrpc", "2.0");
            multiCallRequestBody.put("method", "ExecuteMultiCall");
            Map<String, Object> multiCallParams = new HashMap<>();
            multiCallParams.put("credentials", getAuthenticatedCredentials()); // Credentials go here
            multiCallParams.put("calls", multiCalls); // The list of individual calls
            multiCallRequestBody.put("params", multiCallParams);
            multiCallRequestBody.put("id", requestIdCounter.getAndIncrement()); // Overall multi-call request ID

            LOG.info("Sending " + multiCalls.size() + " sub-calls in a single ExecuteMultiCall request.");
            JsonNode responseNode = geotabApiClient.call(multiCallRequestBody);

            if (responseNode.has("result")) {
                JsonNode resultNode = responseNode.get("result");
                TypeFactory typeFactory = objectMapper.getTypeFactory();

                if (resultNode.isArray()) {
                    System.out.println("  Response is an array of items.");
                    // Normal case: array of responses
                } else {
                    throw new IOException("Unexpected result structure: " + resultNode.toString());
                }


                Map<String, List<LogRecord>> deviceLogRecords = new ConcurrentHashMap<>();
                Map<String, List<StatusData>> deviceOdometerData = new ConcurrentHashMap<>();

                // Process each response item
                for(int i = 0; i < resultNode.size(); i++) {
                    JsonNode subResultNode = resultNode.get(i);
                    JsonNode id = subResultNode.get("id");
                    Integer requestId = null;
                    if (id == null) {
                        MultiCallItem callItem = multiCalls.get(i);
                        requestId = callItem.getId();
                        LOG.info("  Warning: Response item has null id. Getting from request: " + requestId);
                    }
                    if( requestId == null) {
                        LOG.info("  Warning: Response item has null id and no corresponding request ID found. Skipping.");
                        continue; // Skip this item if it has no ID
                    }
                    Map.Entry<String, String> metadata = requestMetadata.get(requestId);
                    if (metadata == null) {
                        LOG.info("  Warning: Received response for unknown request ID: " + requestId);
                        continue;
                    }


                    String deviceId = metadata.getKey();
                    String feedType = metadata.getValue();


                    // Deserialize the actual FeedResult from the 'result' node of the MultiCallResponseItem
                    if (LOG_RECORD_FEED_TYPE.equals(feedType)) {
                        FeedResult<LogRecord> feedResult = objectMapper.treeToValue(
                                subResultNode,
                                typeFactory.constructParametricType(FeedResult.class, LogRecord.class)
                        );
                        List<LogRecord> newRecords = feedResult.getData() != null ? feedResult.getData() : Collections.emptyList();
                        deviceLogRecords.put(deviceId, newRecords);
                        if (feedResult.getToVersion() != null) {
                            lastProcessedVersions.computeIfAbsent(deviceId, k -> new ConcurrentHashMap<>())
                                    .put(LOG_RECORD_FEED_TYPE, feedResult.getToVersion());
                        }
                        LOG.info("  Device " + deviceId + ": Retrieved " + newRecords.size() + " LogRecords.");

                    } else if (STATUS_DATA_FEED_TYPE.equals(feedType)) {
                        FeedResult<StatusData> feedResult = objectMapper.treeToValue(
                                subResultNode,
                                typeFactory.constructParametricType(FeedResult.class, StatusData.class)
                        );
                        // Filter to ensure it's Odometer specifically
                        List<StatusData> newRecords = feedResult.getData() != null ?
                                feedResult.getData().stream()
                                        .filter(sd -> sd.getDiagnostic() != null && DIAGNOSTIC_ODOMETER_ID.equals(sd.getDiagnostic().getId()))
                                        .collect(Collectors.toList()) :
                                Collections.emptyList();
                        deviceOdometerData.put(deviceId, newRecords);
                        if (feedResult.getToVersion() != null) {
                            lastProcessedVersions.computeIfAbsent(deviceId, k -> new ConcurrentHashMap<>())
                                    .put(STATUS_DATA_FEED_TYPE, feedResult.getToVersion());
                        }
                        LOG.info("  Device " + deviceId + ": Retrieved " + newRecords.size() + " Odometer StatusData.");
                    }
                }

                // Now that all data is fetched, proceed with merging and appending for each device
                for (Device device : discoveredVehicles.values()) {
                    List<LogRecord> logRecordsToProcess = deviceLogRecords.getOrDefault(device.getId(), Collections.emptyList());
                    List<StatusData> odometerDataToProcess = deviceOdometerData.getOrDefault(device.getId(), Collections.emptyList());

                    if (!logRecordsToProcess.isEmpty()) {
                        LOG.info("  Merging and appending for Device ID: " + device.getId());
                        List<EnrichedLogRecord> enrichedRecords = matchLogRecordsWithOdometer(device, logRecordsToProcess, odometerDataToProcess);
                        appendEnrichedRecordsToCsv(device, enrichedRecords);
                    } else {
                        LOG.info("  No new LogRecords to enrich for Device ID: " + device.getId());
                    }
                }

            } else if (responseNode.has("error")) {
                throw new IOException("Geotab API error during ExecuteMultiCall: " + responseNode.get("error").toString());
            } else {
                throw new IOException("Unexpected Geotab API response during ExecuteMultiCall: " + responseNode.toString());
            }

            saveLastProcessedVersions(); // Save state after processing all vehicles
            LOG.info("--- Vehicle data backup completed ---");
        } catch (Exception e) {
            LOG.info("Error during scheduled vehicle data backup: " + e.getMessage());
            e.printStackTrace();
        }
        reverseVehicles();
    }

    /**
     * Helper to create parameters for GetFeed calls.
     * @param device The device for the search.
     * @param feedType The type of feed (e.g., "LogRecord", "StatusData").
     * @param diagnosticId (Optional) The diagnostic ID for StatusData (e.g., "DiagnosticOdometerId").
     * @return Map of parameters for the GetFeed method.
     */
    private GeotabApiRequest.Params createGetFeedParams(Device device, String feedType, String diagnosticId) {
        GeotabApiRequest.Params params = new GeotabApiRequest.Params();
        params.setTypeName(feedType);

        String fromVersion = lastProcessedVersions.computeIfAbsent(device.getId(), k -> new ConcurrentHashMap<>())
                .get(feedType);

        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("deviceSearch", Map.of("id", device.getId()));

        if (diagnosticId != null) {
            searchParams.put("diagnosticSearch", Map.of("id", diagnosticId));
        }

        if (fromVersion != null) {
            params.setFromVersion(fromVersion);
        } else {
            // For first run, get data from the last 24 hours
            OffsetDateTime twentyFourHoursAgo = OffsetDateTime.now(ZoneOffset.UTC).minusHours(24);
            searchParams.put("fromDate", twentyFourHoursAgo.format(ISO_Z_FORMATTER));
        }
        params.setSearch(searchParams);
        params.setResultsLimit(50000); // Max for most feeds

        return params;
    }

    // Overloaded helper for LogRecord (no diagnostic ID)
    private GeotabApiRequest.Params createGetFeedParams(Device device, String feedType) {
        return createGetFeedParams(device, feedType, null);
    }


    /**
     * Matches LogRecords with the closest Odometer StatusData by timestamp.
     * Prefers odometer readings that occur at or before the log record's timestamp.
     *
     * @param device The device being processed.
     * @param logRecords The list of LogRecords to enrich (these are typically new records from the current fetch).
     * @param odometerData The list of available Odometer StatusData for matching (these are new records from the current fetch).
     * @return A list of EnrichedLogRecord objects.
     */
    private List<EnrichedLogRecord> matchLogRecordsWithOdometer(Device device, List<LogRecord> logRecords, List<StatusData> odometerData) {
        LOG.info("  Matching " + logRecords.size() + " LogRecords with " + odometerData.size() + " Odometer data points.");

        List<EnrichedLogRecord> enrichedRecords = new ArrayList<>();
        String vin = device.getVehicleIdentificationNumber();

        // Sort odometer data by timestamp for efficient searching
        odometerData.sort(Comparator.comparing(StatusData::getDateTime));

        for (LogRecord log : logRecords) {
            Double matchedOdometerValue = null;
            StatusData closestOdometer = null;
            Duration minDiff = ODOMETER_MATCH_THRESHOLD; // Max allowed difference

            // Iterate through sorted odometer data to find the closest value
            // We search for an odometer reading *at or before* the log record's timestamp
            for (StatusData odometer : odometerData) {
                // If the odometer reading is after the log record, we can stop, as subsequent ones will also be after
                if (odometer.getDateTime().isAfter(log.getDateTime())) {
                    break;
                }

                Duration diff = Duration.between(odometer.getDateTime(), log.getDateTime()).abs();

                // Find the closest odometer reading *before or at* the log record's timestamp
                // If multiple are equally close, the one closest to log.dateTime (which would be later in time but still <= log.dateTime) is chosen
                if (diff.compareTo(minDiff) < 0) { // If this one is closer than the current minDiff
                    closestOdometer = odometer;
                    minDiff = diff;
                }
            }

            if (closestOdometer != null) {
                matchedOdometerValue = closestOdometer.getData();
            }

            enrichedRecords.add(new EnrichedLogRecord(
                    device.getId(),
                    vin,
                    log.getDateTime(),
                    log.getLatitude(),
                    log.getLongitude(),
                    matchedOdometerValue
            ));
        }
        LOG.info("  Created " + enrichedRecords.size() + " enriched records.");
        return enrichedRecords;
    }

    /**
     * Processes LogRecord data for a single vehicle using GetFeed.
     *
     * @param device The Device object to process.
     * @return
     * @throws IOException If an API or file operation error occurs.
     */
    /*
    private List<LogRecord> processLogRecordsForVehicle(Device device) throws IOException {
        LOG.info("Processing LogRecords for Device ID: " + device.getId() + " (" + device.getName() + ")");

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
            LOG.info("  Using fromVersion: " + fromVersion);
        } else {
            // If no version (first run for this device/feed), use fromDate to seed data
            // Default to getting data from the last 24 hours (adjust as needed for demo data)
            OffsetDateTime twentyFourHoursAgo = OffsetDateTime.now(ZoneOffset.UTC).minusHours(24);
            searchParams.put("fromDate", twentyFourHoursAgo.format(ISO_Z_FORMATTER));
            LOG.info("  No existing version. Seeding fromDate: " + twentyFourHoursAgo);
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
                LOG.info("  Retrieved " + newLogRecords.size() + " new LogRecords.");
            } else {
                LOG.info("  No new LogRecords found.");
            }
            // Even if no new data, update toVersion to current state to avoid re-fetching old empty range
            // This is crucial for GetFeed to advance correctly even with no new records.
            if (newToVersion != null) {
                lastProcessedVersions.computeIfAbsent(device.getId(), k -> new ConcurrentHashMap<>())
                        .put(LOG_RECORD_FEED_TYPE, newToVersion);
            }

        } else if (responseNode.has("error")) {
            LOG.info("  Error fetching LogRecords for device " + device.getId() + ": " + responseNode.get("error").toString());
        } else {
            LOG.info("  Unexpected API response for device " + device.getId() + ": " + responseNode.toString());
        }

        return newLogRecords;
    }

     */

    /**
     * Fetches new StatusData (Odometer) for a single vehicle using GetFeed.
     * @param device The Device object to process.
     * @return List of newly retrieved StatusData objects filtered for Odometer.
     * @throws IOException If an API error occurs.
     */
    /*
    private List<StatusData> processStatusDataForVehicle(Device device) throws IOException {
        LOG.info("  Fetching Odometer StatusData...");

        List<StatusData> newOdometerStatusData = Collections.emptyList();

        GeotabApiRequest.Params params = new GeotabApiRequest.Params();
        params.setTypeName(STATUS_DATA_FEED_TYPE);

        String fromVersion = lastProcessedVersions.computeIfAbsent(device.getId(), k -> new ConcurrentHashMap<>())
                .get(STATUS_DATA_FEED_TYPE);

        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("deviceSearch", Map.of("id", device.getId()));
        searchParams.put("diagnosticSearch", Map.of("id", DIAGNOSTIC_ODOMETER_ID)); // Filter for Odometer data

        if (fromVersion != null) {
            params.setFromVersion(fromVersion);
            LOG.info("    Odometer: Using fromVersion: " + fromVersion);
        } else {
            OffsetDateTime twentyFourHoursAgo = OffsetDateTime.now(ZoneOffset.UTC).minusHours(24); // Adjust as needed
            searchParams.put("fromDate", twentyFourHoursAgo.format(ISO_Z_FORMATTER));
            LOG.info("    Odometer: No existing version. Seeding fromDate: " + twentyFourHoursAgo);
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
                LOG.info("    Retrieved " + newOdometerStatusData.size() + " new Odometer StatusData.");
            } else {
                LOG.info("    No new Odometer StatusData found.");
            }

            // Always update toVersion
            if (newToVersion != null) {
                lastProcessedVersions.computeIfAbsent(device.getId(), k -> new ConcurrentHashMap<>())
                        .put(STATUS_DATA_FEED_TYPE, newToVersion);
            }

        } else if (responseNode.has("error")) {
            LOG.info("    Error fetching Odometer StatusData for device " + device.getId() + ": " + responseNode.get("error").toString());
        } else {
            LOG.info("    Unexpected API response for Odometer StatusData for device " + device.getId() + ": " + responseNode.toString());
        }

        return newOdometerStatusData;
    }

     */

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
            LOG.info("  Appended " + enrichedRecords.size() + " enriched records to " + filename);
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
        LOG.info("Making Geotab API 'Get' call for 'Device' type using authenticated session...");

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

            LOG.info("Successfully retrieved and stored " + devices.size() + " devices from Geotab API.");
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
