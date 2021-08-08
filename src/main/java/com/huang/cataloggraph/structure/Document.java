package com.huang.cataloggraph.structure;

import java.util.Map;

/**
 *
 **/
public class Document {
    private String id;
    private Map<String, Object> properties;

    public Document(String id, Map<String, Object> properties) {
        this.id = id;
        this.properties = properties;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
