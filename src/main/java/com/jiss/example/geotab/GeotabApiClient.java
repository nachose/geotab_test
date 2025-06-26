package com.jiss.example.geotab;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

@RegisterRestClient(configKey="geotab-api")
@Path("/apiv1") // Assuming this is the base path for your API calls
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface GeotabApiClient {

    // This method will handle authentication and subsequent API calls
    // Geotab API typically uses a single POST endpoint for all calls
    // The 'method' and 'params' are part of the JSON payload
    @POST
    String call(Map<String, Object> requestBody); // You'll parse the response manually or use a more specific return type
}
