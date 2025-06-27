package com.jiss.example.geotab.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents a Vehicle (Device) entity from the Geotab API.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Device {
    public String id;
    public String name;
    public String vehicleIdentificationNumber; // The VIN

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVehicleIdentificationNumber() {
        return vehicleIdentificationNumber;
    }

    public void setVehicleIdentificationNumber(String vehicleIdentificationNumber) {
        this.vehicleIdentificationNumber = vehicleIdentificationNumber;
    }

    @Override
    public String toString() {
        return "Device{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", VIN='" + vehicleIdentificationNumber + '\'' +
                '}';
    }
}
