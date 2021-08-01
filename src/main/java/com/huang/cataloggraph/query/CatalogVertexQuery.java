package com.huang.cataloggraph.query;

import com.huang.cataloggraph.structure.CatalogEdge;
import com.huang.cataloggraph.structure.CatalogVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 *
 **/
public interface CatalogVertexQuery {
    CatalogVertexQuery direction(Direction direction);

    CatalogVertexQuery label(String... label);

    CatalogVertexQuery has(String key, Object value);

    CatalogQueryResult<CatalogVertex> vertices();

    CatalogQueryResult<CatalogEdge> edges();
}
