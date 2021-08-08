package com.huang.cataloggraph.structure;

import com.huang.cataloggraph.config.GraphConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.*;

/**
 *
 **/
public class CatalogEsTransaction implements CatalogTransaction {
    private CatalogGraph graph;

    private volatile boolean isOpen;

    private Map<String, CatalogVertex> vertexMap;

    private Map<String, CatalogEdge> edgeMap;

    public CatalogEsTransaction(CatalogGraph graph) {
        this.graph = graph;
        this.isOpen = true;
        this.vertexMap = new HashMap<>();
        this.edgeMap = new HashMap<>();
    }

    public CatalogGraph getGraph() {
        return graph;
    }

    public boolean isOpen() {
        return isOpen;
    }

    public CatalogVertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object id = ElementHelper.getIdValue(keyValues).orElse(null);
        String label = ElementHelper.getLabelValue(keyValues).orElse("vertex");

        if (id == null) {
            id = UUID.randomUUID();
        }

        CatalogVertex vertex = new CatalogVertex(this, id, label);
        ElementHelper.attachProperties(vertex, keyValues);
        vertexMap.put(vertex.id().toString(), vertex);
        return vertex;
    }

    public CatalogEdge addEdge(String label, CatalogVertex outVertex, CatalogVertex inVertex, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object id = ElementHelper.getIdValue(keyValues).orElse(null);

        if (id == null) {
            id = UUID.randomUUID();
        }

        CatalogEdge edge = new CatalogEdge(this, id, label, outVertex, inVertex);
        ElementHelper.attachProperties(edge, keyValues);
        edgeMap.put(edge.id().toString(), edge);
        return edge;
    }

    public Iterator<Vertex> vertices(Object... vertexIds) {
        List<CatalogVertex> ret = new ArrayList<>(vertexIds.length);

        List<String> docIds = new ArrayList<>(vertexIds.length);
        List<Integer> pos = new ArrayList<>(vertexIds.length);

        for (int i = 0; i < vertexIds.length; i++) {
            CatalogVertex vertex = vertexMap.get(vertexIds[i].toString());
            if (vertex == null) {
                docIds.add(vertexIds[i].toString());
                pos.add(i);
            } else {
                ret.set(i, vertex);
            }
        }

        if (!docIds.isEmpty()) {
            List<Document> docs = getDoc(docIds, "vertex");
            for (int i = 0; i < docs.size(); i++) {
                Document document = docs.get(i);
                ret.add(pos.get(i), GraphObjectFactory.createVertex(this, document));
            }
        }

        return (Iterator) ret.iterator();
    }

    public Iterator<Edge> edges(Object... edgeIds) {
        List<CatalogEdge> ret = new ArrayList<>(edgeIds.length);

        List<String> docIds = new ArrayList<>(edgeIds.length);
        List<Integer> pos = new ArrayList<>(edgeIds.length);

        for (int i = 0; i < edgeIds.length; i++) {
            CatalogEdge edge = edgeMap.get(edgeIds[i].toString());
            if (edge == null) {
                docIds.add(edgeIds[i].toString());
                pos.add(i);
            } else {
                ret.set(i, edge);
            }
        }

        if (!docIds.isEmpty()) {
            List<Document> docs = getDoc(docIds, "edge");
            for (int i = 0; i < docs.size(); i++) {
                Document document = docs.get(i);
                ret.add(pos.get(i), GraphObjectFactory.createEdge(this, document));
            }
        }

        return (Iterator) ret.iterator();
    }

    public void commit() {
        RestHighLevelClient client = graph.client.getClient();

        BulkRequest request = new BulkRequest();
        addBulkRequest(request, vertexMap.values(), GraphConfiguration.getVertex(graph.configuration()));
        addBulkRequest(request, edgeMap.values(), GraphConfiguration.getEdge(graph.configuration()));

        if (request.numberOfActions() > 0) {
            try {
                BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
                if (response.hasFailures()) {
                    throw new RuntimeException("Execute bulk request failed: " + response.buildFailureMessage());
                }
            } catch (IOException e) {
                throw new RuntimeException("Execute bulk request failed ", e);
            } finally {
                closeTx();
            }
        }
    }

    public void rollback() {
        // do nothing
        closeTx();
    }

    private void closeTx() {
        isOpen = false;
        graph.getOpenTransactions().remove(this);
    }

    private List<Document> getDoc(List<String> docIds, String index) {
        List<Document> ret = new ArrayList<>(docIds.size());

        RestHighLevelClient client = graph.client.getClient();

        MultiGetRequest request = new MultiGetRequest();

        if (StringUtils.equals(index, "vertex")) {
            docIds.forEach(id -> request.add(GraphConfiguration.getVertex(graph.configuration()), id));
        } else {
            docIds.forEach(id -> request.add(GraphConfiguration.getEdge(graph.configuration()), id));
        }

        try {
            MultiGetResponse responses = client.mget(request, RequestOptions.DEFAULT);
            for (MultiGetItemResponse getResponse : responses.getResponses()) {
                GetResponse response = getResponse.getResponse();
                if (response.isExists()) {
                    ret.add(new Document(response.getId(), response.getSourceAsMap()));
                } else {
                    ret.add(null);
                }
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException("Execute get request failed", e);
        }
    }

    private void addBulkRequest(BulkRequest bulkRequest, Collection<? extends CatalogElement> collection, String index) {
        collection.stream().peek(e -> {
            e.getProperties().put(CatalogElement.LABEL, e.label());
            if (e instanceof CatalogEdge) {
                e.getProperties().put(CatalogEdge.ENDPOINT1_ID, ((CatalogEdge) e).getOutVertexId());
                e.getProperties().put(CatalogEdge.ENDPOINT2_ID, ((CatalogEdge) e).getInVertexId());
            }
        }).forEach(v -> {
            if (v.removed) {
                bulkRequest.add(new DeleteRequest(index).id(v.id().toString()));
            } else {
                bulkRequest.add(new UpdateRequest(index, v.id().toString()).upsert(v.getProperties()).doc(v.getProperties()));
            }
        });
    }
}
