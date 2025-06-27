package com.jiss.example.geotab.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents the successful response from the Geotab API's 'Authenticate' method.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LoginResult {
    private Credentials credentials;
    private String path;

    public Credentials getCredentials() {
        return credentials;
    }

    public void setCredentials(Credentials credentials) {
        this.credentials = credentials;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getSessionId() {
        return credentials != null ? credentials.getSessionId() : null;
    }

    public String getUserName() {
        return credentials != null ? credentials.getUserName() : null;
    }

    public String getDatabase() {
        return credentials != null ? credentials.getDatabase() : null;
    }

    @Override
    public String toString() {
        return "LoginResult{" +
                "credentials=" + credentials +
                ", path='" + path + '\'' +
                '}';
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Credentials {
        private String database;
        private String sessionId;
        private String userName;

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getSessionId() {
            return sessionId;
        }

        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        @Override
        public String toString() {
            return "Credentials{" +
                    "database='" + database + '\'' +
                    ", sessionId='" + sessionId + '\'' +
                    ", userName='" + userName + '\'' +
                    '}';
        }
    }
}
