package com.huang.cataloggraph.query;

import com.huang.cataloggraph.structure.CatalogEdge;
import com.huang.cataloggraph.structure.CatalogElement;
import com.huang.cataloggraph.structure.CatalogEsTransaction;
import com.huang.cataloggraph.structure.CatalogVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 **/
public class CatalogEsVertexQuery implements CatalogVertexQuery {
    private CatalogVertex vertex;

    private CatalogGraphQuery query;

    private CatalogEsTransaction tx;

    public CatalogEsVertexQuery(CatalogVertex vertex, CatalogEsTransaction tx) {
        this.vertex = vertex;
        this.tx = tx;
        this.query = new CatalogEsGraphQuery(tx);
    }

    @Override
    public CatalogVertexQuery direction(Direction direction) {
        if (direction == Direction.IN) {
            query.has(CatalogEdge.ENDPOINT2_ID, vertex.id());
        } else if (direction == Direction.OUT) {
            query.has(CatalogEdge.ENDPOINT1_ID, vertex.id());
        } else {
            List<CatalogGraphQuery> orConditions = new LinkedList<>();
            orConditions.add(query.has(CatalogEdge.ENDPOINT2_ID, vertex.id()));
            orConditions.add(query.has(CatalogEdge.ENDPOINT1_ID, vertex.id()));
            query.or(orConditions);
        }
        return this;
    }

    @Override
    public CatalogVertexQuery label(String... label) {
        query.in(CatalogElement.LABEL, Arrays.asList(label));
        return this;
    }

    @Override
    public CatalogVertexQuery has(String key, Object value) {
        query.has(key, value);
        return this;
    }

    @Override
    public CatalogQueryResult<CatalogVertex> vertices() {
        CatalogQueryResult<CatalogEdge> edges = query.edges();
        Object[] id = edges.getStream().map(e -> {
            if (e.getInVertexId().equals(vertex.id().toString())) {
                return e.getOutVertexId();
            } else {
                return e.getInVertexId();
            }
        }).collect(Collectors.toList()).toArray(new Object[1]);

        Stream stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(tx.vertices(id), Spliterator.ORDERED), false);

        return new CatalogQueryResult<>(edges.getCount(), (Stream<CatalogVertex>) stream);
    }

    @Override
    public CatalogQueryResult<CatalogEdge> edges() {
        return query.edges();
    }
}
