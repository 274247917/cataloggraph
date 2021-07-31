package com.huang.cataloggraph.structure;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public interface CatalogTransaction {
    CatalogGraph getGraph();

    boolean isOpen();

    CatalogVertex addVertex(Object... keyValues);

    CatalogEdge addEdge(String label, CatalogVertex outVertex, CatalogVertex inVertex, Object... keyValues);

    Iterator<Vertex> vertices(Object... vertexIds);

    Iterator<Edge> edges(Object... edgeIds);

    void commit();

    void rollback();
}
