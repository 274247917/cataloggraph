package com.huang.cataloggraph.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * vertex property在 tinkerpop 的设计中可以定义成像element一样的一个实体，我们实际实现中还是将其作为vertex的属性，不额外存储
 **/
public class CatalogVertexProperty<V> extends CatalogElement implements VertexProperty<V> {
    private final CatalogVertex vertex;
    private final String key;
    private final V value;

    public CatalogVertexProperty(CatalogVertex vertex, String key, V value) {
        super(vertex.tx, UUID.randomUUID(), key);

        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return Collections.emptyIterator();
    }
}
