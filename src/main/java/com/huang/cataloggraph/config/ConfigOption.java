package com.huang.cataloggraph.config;

/**
 *
 **/
public class ConfigOption<T> {
    private String key;
    private T defaultValue;
    private Class<?> clazz;

    public ConfigOption(String key, Class<?> clazz) {
        this.key = key;
        this.clazz = clazz;
    }

    public ConfigOption(String key, T defaultValue, Class<?> clazz) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = clazz;
    }

    public String getKey() {
        return key;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public Class<?> getClazz() {
        return clazz;
    }
}
