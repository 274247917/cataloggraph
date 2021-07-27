package com.huang.cataloggraph.config;

import org.apache.commons.configuration.BaseConfiguration;

/**
 *
 **/
public class CatalogConfiguration extends BaseConfiguration {
    public <T> T get(ConfigOption<T> configOption) {
        if (configOption.getClazz() == String.class) {
            return getValue(getString(configOption.getKey()), configOption);
        }
        throw new IllegalArgumentException("Unrecognized configuration type");
    }

    private <T> T getValue(Object value, ConfigOption<T> configOption) {
        if (value == null) {
            return configOption.getDefaultValue();
        }
        return (T) value;
    }
}
