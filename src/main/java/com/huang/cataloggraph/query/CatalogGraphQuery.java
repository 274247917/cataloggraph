package com.huang.cataloggraph.query;

import com.huang.cataloggraph.structure.CatalogEdge;
import com.huang.cataloggraph.structure.CatalogGraph;
import com.huang.cataloggraph.structure.CatalogVertex;

import java.util.Collection;
import java.util.List;

public interface CatalogGraphQuery {
    CatalogGraphQuery has(String propertyKey);

    CatalogGraphQuery has(String propertyKey, Object value);

    CatalogGraphQuery has(String propertyKey, QueryOperator operator, Object value);

    CatalogGraphQuery in(String propertyKey, Collection<?> values);

    CatalogGraphQuery or(List<CatalogGraphQuery> childQueries);

    CatalogGraphQuery limit(int limit);

    CatalogGraphQuery offset(int offset);

    CatalogQueryResult<CatalogVertex> vertices();

    CatalogQueryResult<CatalogEdge> edges();

    enum ComparisionOperator implements QueryOperator {
        GREATER_THAN,
        GREATER_THAN_EQUAL,
        EQUAL,
        LESS_THAN,
        LESS_THAN_EQUAL,
        NOT_EQUAL
    }

    enum MatchingOperator implements QueryOperator {
        PREFIX,
        REGEX,
        MATCH
    }

    interface QueryOperator{}
}
