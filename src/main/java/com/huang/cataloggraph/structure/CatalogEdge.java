package com.huang.cataloggraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 **/
public class CatalogEdge extends CatalogElement implements Edge {
    public static final String ENDPOINT1_ID = "ep1Id";

    public static final String ENDPOINT2_ID = "ep2Id";

    private String outVertexId;

    private String inVertexId;

    private CatalogVertex outVertex;

    private CatalogVertex inVertex;

    public CatalogEdge(CatalogEsTransaction tx, Object id, String label, String outVertexId, String inVertexId) {
        super(tx, id, label);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
    }

    public CatalogEdge(CatalogEsTransaction tx, Object id, String label, CatalogVertex outVertex, CatalogVertex inVertex) {
        super(tx, id, label);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.outVertexId = outVertex.id.toString();
        this.inVertexId = inVertex.id.toString();
    }

    public String getOutVertexId() {
        return outVertexId;
    }

    public String getInVertexId() {
        return inVertexId;
    }

    @Override
    public Vertex outVertex() {
        return outVertex != null ? outVertex : tx().vertices(outVertexId).next();
    }

    @Override
    public Vertex inVertex() {
        return inVertex != null ? inVertex : tx().vertices(inVertexId).next();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (removed) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex);
            case IN:
                return IteratorUtils.of(this.inVertex);
            default:
                return IteratorUtils.of(this.outVertex, this.inVertex);
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        CatalogProperty<V> property = new CatalogProperty<>(this, key, value);
        this.properties.put(key, value);
        return property;
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<CatalogProperty<Object>> properties = Arrays.stream(propertyKeys)
                .filter(this.properties::containsKey)
                .map(p -> new CatalogProperty<>(this, p, this.properties.get(p)))
                .collect(Collectors.toList());
        return (Iterator)properties.iterator();
    }
}
