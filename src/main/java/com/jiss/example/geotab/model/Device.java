package com.jiss.example.geotab.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Device {
    public String id;
    public String name; // This might be "Vehicle ID"
    public String vehicleIdentificationNumber; // This is the VIN
    // Add other fields you might need from the Device object
}
