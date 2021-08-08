package com.huang.cataloggraph.process.traversal.step;

import com.huang.cataloggraph.query.CatalogGraphQuery;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;

/**
 *
 **/
public class CatalogGraphPredicateUtil {
    public static CatalogGraphQuery.ComparisionOperator convert(Compare p) {
        switch (p) {
            case eq:
                return CatalogGraphQuery.ComparisionOperator.EQUAL;
            case neq:
                return CatalogGraphQuery.ComparisionOperator.NOT_EQUAL;
            case gt:
                return CatalogGraphQuery.ComparisionOperator.GREATER_THAN;
            case gte:
                return CatalogGraphQuery.ComparisionOperator.GREATER_THAN_EQUAL;
            case lt:
                return CatalogGraphQuery.ComparisionOperator.LESS_THAN;
            case lte:
                return CatalogGraphQuery.ComparisionOperator.LESS_THAN_EQUAL;
        }
        throw new IllegalArgumentException("Unknown Operator");
    }
}
