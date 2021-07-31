package com.huang.cataloggraph.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;

/**
 *
 **/
public abstract class CatalogElement implements Element {
    public static final String LABEL = "label";

    protected final Object id;

    protected final String label;

    protected CatalogEsTransaction tx;

    protected boolean removed = false;

    protected Map<String, Object> properties;

    public CatalogElement(CatalogEsTransaction tx, Object id, String label) {
        this.id = id;
        this.label = label;
        this.tx = tx;
        this.properties = new HashMap<>();
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Graph graph() {
        return tx.getGraph();
    }

    @Override
    public void remove() {
        this.removed = true;
    }

    protected CatalogTransaction tx() {
        return tx.getGraph().getCurrentThreadTx();
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
