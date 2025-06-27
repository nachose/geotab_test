package com.jiss.example.geotab;

import com.fasterxml.jackson.databind.JsonNode;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;

/**
 * MicroProfile REST Client for the Geotab API.
 * The 'configKey' points to 'geotab-api' in application.properties for its base URI.
 * All Geotab API calls are POST requests to a single endpoint, with method and params
 * specified in the JSON body.
 */
@RegisterRestClient(configKey="geotab-api")
@Path("/") // The base URI from configKey includes the /apiv1, so this path is relative to that.
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface GeotabApiClient {

    /**
     * Executes a generic Geotab API call.
     * The requestBody should be a map representing the JSON-RPC request structure
     * (e.g., {"jsonrpc": "2.0", "method": "Get", "params": {...}, "id": 1}).
     * Returns a JsonNode to allow flexible parsing of different response structures.
     *
     * @param requestBody The request payload as a Map.
     * @return A JsonNode representing the full JSON-RPC response.
     */
    @POST
    JsonNode call(Map<String, Object> requestBody);
}
