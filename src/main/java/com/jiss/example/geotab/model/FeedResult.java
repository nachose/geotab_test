package com.jiss.example.geotab.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * Generic model for the response structure of a Geotab 'GetFeed' API call.
 * @param <T> The type of entities contained in the 'data' list.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedResult<T> {
    public List<T> data;
    public String toVersion;

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public String getToVersion() {
        return toVersion;
    }

    public void setToVersion(String toVersion) {
        this.toVersion = toVersion;
    }

    @Override
    public String toString() {
        return "FeedResult{" +
                "dataSize=" + (data != null ? data.size() : 0) +
                ", toVersion='" + toVersion + '\'' +
                '}';
    }
}
