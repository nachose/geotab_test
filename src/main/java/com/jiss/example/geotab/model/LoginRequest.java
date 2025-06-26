package com.jiss.example.geotab.model;

public class LoginRequest {
    private String database;
    private String userName;
    private String password;
    private String sessionId;

    public LoginRequest() {
    }

    public LoginRequest(String database, String userName, String password, String sessionId) {
        this.database = database;
        this.userName = userName;
        this.password = password;
        this.sessionId = sessionId;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
