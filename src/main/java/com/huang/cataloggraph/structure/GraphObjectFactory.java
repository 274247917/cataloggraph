package com.huang.cataloggraph.structure;

/**
 *
 **/
public class GraphObjectFactory {
    public static CatalogVertex createVertex(CatalogEsTransaction tx, Document document) {
        if (document == null) {
            return null;
        }

        CatalogVertex vertex = new CatalogVertex(tx, document.getId(), document.getProperties().remove(CatalogElement.LABEL).toString());
        document.getProperties().forEach(vertex::property);
        return vertex;
    }

    public static CatalogEdge createEdge(CatalogEsTransaction tx, Document document) {
        if (document == null) {
            return null;
        }

        CatalogEdge edge = new CatalogEdge(tx, document.getId(),
                document.getProperties().remove(CatalogElement.LABEL).toString(),
                document.getProperties().remove(CatalogEdge.ENDPOINT1_ID).toString(),
                document.getProperties().remove(CatalogEdge.ENDPOINT2_ID).toString());
        document.getProperties().forEach(edge::property);
        return edge;
    }
}
