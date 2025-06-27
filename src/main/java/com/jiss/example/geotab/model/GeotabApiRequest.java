package com.jiss.example.geotab.model;

import java.util.Map;

/**
 * Helper class to build the JSON-RPC 2.0 request payload for Geotab API calls.
 * Right now used for handling both Get and GetFeed request, as the parameters are
 * similar, although not completely identical.
 * In a professional setting, probably have two classes, even if there is some code repetition.
 */
public class GeotabApiRequest {
    private String jsonrpc = "2.0"; // Standard JSON-RPC version
    private String method;           // The API method to call (e.g., "Get", "GetFeed")
    private Params params;
    private int id = 1;              // Request ID, can be a static value for simple apps

    public static class Params {
        private String typeName;
        private Object search; // or specific type if needed
        private LoginResult.Credentials credentials;
        private String fromVersion;
        private int resultsLimit;

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public Object getSearch() {
            return search;
        }

        public void setSearch(Object search) {
            this.search = search;
        }

        public LoginResult.Credentials getCredentials() {
            return credentials;
        }

        public void setCredentials(LoginResult.Credentials credentials) {
            this.credentials = credentials;
        }

        public String getFromVersion() {
            return fromVersion;
        }

        public void setFromVersion(String fromVersion) {
            this.fromVersion = fromVersion;
        }

        public int getResultsLimit() {
            return resultsLimit;
        }

        public void setResultsLimit(int resultsLimit) {
            this.resultsLimit = resultsLimit;
        }
    }

    public GeotabApiRequest(String method, Params params) {
        this.method = method;
        this.params = params;
    }

    public String getJsonrpc() {
        return jsonrpc;
    }

    public void setJsonrpc(String jsonrpc) {
        this.jsonrpc = jsonrpc;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Params getParams() {
        return params;
    }

    public void setParams(Params params) {
        this.params = params;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

}
