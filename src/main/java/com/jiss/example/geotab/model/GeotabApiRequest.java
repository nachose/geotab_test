package com.jiss.example.geotab.model;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Helper class to build the JSON-RPC 2.0 request payload for Geotab API calls.
 * Right now used for handling both Get and GetFeed request, as the parameters are
 * similar, although not completely identical.
 * In a professional setting, probably have two classes, even if there is some code repetition.
 */
public class GeotabApiRequest {
    private String jsonrpc = "2.0";
    private String method;           // The API method to call (e.g., "Get", "GetFeed")
    private Params params;

    private static int counter = 1;
    private int id = counter++;

    public static class Params {
        private String typeName;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        private Object search; // or specific type if needed
        private LoginResult.Credentials credentials;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String fromVersion;

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
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
