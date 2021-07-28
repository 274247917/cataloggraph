package com.huang.cataloggraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 **/
public class CatalogVertex extends CatalogElement implements Vertex {
    public CatalogVertex( CatalogEsTransaction tx, Object id, String label) {
        super(tx, id, label);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        return tx().addEdge(label, this, (CatalogVertex) inVertex, keyValues);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        CatalogVertexProperty<V> vertexProperty = new CatalogVertexProperty<>(this, key, value);
        this.properties.put(key, value);
        return vertexProperty;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return null;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<CatalogVertexProperty<Object>> properties = Arrays.stream(propertyKeys)
                .filter(this.properties::containsKey)
                .map(p -> new CatalogVertexProperty<>(this, p, this.properties.get(p)))
                .collect(Collectors.toList());
        return (Iterator)properties.iterator();
    }
}
