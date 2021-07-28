package com.huang.cataloggraph.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.NoSuchElementException;

/**
 *
 **/
public class CatalogProperty<V> implements Property<V> {
    protected final Element element;
    protected final String key;
    protected V value;

    public CatalogProperty(Element element, String key, V value) {
        this.element = element;
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
        return value != null;
    }

    @Override
    public Element element() {
        return element;
    }

    @Override
    public void remove() {
        ((CatalogElement) element).properties.remove(key);
    }
}
