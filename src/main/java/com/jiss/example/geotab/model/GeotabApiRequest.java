package com.jiss.example.geotab.model;

import java.util.Map;

public class GeotabApiRequest {
    public String method;
    public Map<String, Object> params;
    // You might need a "credentials" object here if not handled by a session
    public Map<String, Object> credentials; // This is a common pattern for Geotab API

    public GeotabApiRequest(String method, Map<String, Object> params, Map<String, Object> credentials) {
        this.method = method;
        this.params = params;
        this.credentials = credentials;
    }
}
