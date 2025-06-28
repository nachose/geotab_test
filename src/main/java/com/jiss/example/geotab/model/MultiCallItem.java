package com.jiss.example.geotab.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * Represents a single API method call within a Geotab 'ExecuteMultiCall' request.
 * Does NOT contain credentials, as they are at the top level of ExecuteMultiCall.
 */
@JsonInclude(JsonInclude.Include.NON_NULL) // Only include non-null fields when serializing
public class MultiCallItem {
    public String method;
    public GeotabApiRequest.Params params;
    public Integer id; // Unique ID for this specific call within the multi-call batch

    public MultiCallItem(String method, GeotabApiRequest.Params params, Integer id) {
        this.method = method;
        this.params = params;
        this.id = id;
    }

    // Getters (and setters if Jackson needs them for deserialization, though we are constructing these)
    public String getMethod() { return method; }
    public GeotabApiRequest.Params getParams() { return params; }
    public Integer getId() { return id; }
}
