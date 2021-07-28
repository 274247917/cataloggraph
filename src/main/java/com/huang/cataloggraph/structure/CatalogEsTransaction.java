package com.huang.cataloggraph.structure;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 **/
public class CatalogEsTransaction implements CatalogTransaction {
    private CatalogGraph graph;

    private volatile boolean isOpen;

    private Map<String, CatalogVertex> vertexMap;

    private Map<String, CatalogEdge> edgeMap;

    public CatalogEsTransaction(CatalogGraph graph) {
        this.graph = graph;
        this.isOpen = true;
        this.vertexMap = new HashMap<>();
        this.edgeMap = new HashMap<>();
    }

    public CatalogEsTransaction getNextTx() {
        return null;
    }

    public CatalogGraph getGraph() {
        return graph;
    }

    public boolean isOpen() {
        return isOpen;
    }

    public CatalogVertex addVertex(Object... keyValues) {
        return null;
    }

    public CatalogEdge addEdge(String label, CatalogVertex outVertex, CatalogVertex inVertex, Object... keyValues) {
        return null;
    }

    public Iterator<Vertex> vertices(Object... vertexIds) {
        return null;
    }

    public Iterator<Edge> edges(Object... edges) {
        return null;
    }

    public void commit() {
        RestHighLevelClient client = graph.client.getClient();
    }

    public void rollback() {
        // do nothing
    }
}
