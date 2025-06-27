// LoginResponse.java
package com.jiss.example.geotab.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LoginResponse {
    private LoginResult result;

    public LoginResult getResult() {
        return result;
    }

    public void setResult(LoginResult result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "LoginResponse{" +
                "result=" + result +
                '}';
    }
}