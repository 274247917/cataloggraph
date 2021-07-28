package com.huang.cataloggraph.config;

/**
 *
 **/
public class GraphConfiguration {
    public static final ConfigOption<String> GRAPH_NAME = new ConfigOption<String>("graph.name", "catalog", String.class);

    public static final ConfigOption<String> INDEX_HOSTNAME = new ConfigOption<String>("index.hostname", String.class);

    public static String getVertex(CatalogConfiguration configuration) {
        return configuration.get(GRAPH_NAME) + "_vertex";
    }

    public static String getEdge(CatalogConfiguration configuration) {
        return configuration.get(GRAPH_NAME) + "_edge";
    }
}
