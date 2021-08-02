package com.huang.cataloggraph.structure;

import com.huang.cataloggraph.config.CatalogConfiguration;
import com.huang.cataloggraph.query.CatalogEsGraphQuery;
import com.huang.cataloggraph.query.CatalogGraphQuery;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 **/
public class CatalogGraph implements Graph {
    private volatile boolean isOpen;

    final GraphTransaction tinkerpopTx = new GraphTransaction();

    private ThreadLocal<CatalogTransaction> tx = ThreadLocal.withInitial(() -> null);

    private Set<CatalogTransaction> openTransactions;

    private final CatalogConfiguration configuration;

    ElasticSearchClient client;

    public CatalogGraph(Configuration configuration) {
        this.isOpen = true;
        this.openTransactions = ConcurrentHashMap.newKeySet();
        this.configuration = new CatalogConfiguration();
        Iterator<String> keys = configuration.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            configuration.setProperty(key, configuration.getProperty(key));
        }
        client = new ElasticSearchClient(this.configuration);
    }

    private CatalogTransaction newTransaction() {
        if (!isOpen) {
            throw new IllegalStateException("Graph has been shutdown");
        }
        CatalogTransaction tx = new CatalogEsTransaction(this);
        openTransactions.add(tx);
        return tx;
    }

    /**
     * {@link GraphTransaction#doOpen()}
     */
    private void startNewTx() {
        CatalogTransaction transaction = tx.get();
        if (transaction != null && transaction.isOpen()) {
            throw Transaction.Exceptions.transactionAlreadyOpen();
        }

        transaction = newTransaction();
        tx.set(transaction);
    }

    /**
     * {@link GraphTransaction#readWrite()} finally call {@link GraphTransaction#doOpen()}
     * @return
     */
    public CatalogTransaction getAutoStartTx() {
        if (tx == null) {
            throw new IllegalStateException("Graph has been closed");
        }
        tinkerpopTx.readWrite();
        return tx.get();
    }

    public CatalogTransaction getCurrentThreadTx() {
        if (tx == null) {
            throw new IllegalStateException("Graph has been closed");
        }
        CatalogTransaction transaction = tx.get();
        return (transaction != null && transaction.isOpen()) ? transaction : getAutoStartTx();
    }

    Set<CatalogTransaction> getOpenTransactions() {
        return openTransactions;
    }

    public ElasticSearchClient getClient() {
        return client;
    }

    public CatalogGraphQuery query() {
        return new CatalogEsGraphQuery((CatalogEsTransaction) getCurrentThreadTx());
    }

    public Vertex addVertex(Object... keyValues) {
        return getAutoStartTx().addVertex(keyValues);
    }

    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return null;
    }

    public GraphComputer compute() throws IllegalArgumentException {
        return null;
    }

    public Iterator<Vertex> vertices(Object... vertexIds) {
        return getAutoStartTx().vertices(vertexIds);
    }

    public Iterator<Edge> edges(Object... edgeIds) {
        return getAutoStartTx().edges(edgeIds);
    }

    public Transaction tx() {
        return tinkerpopTx;
    }

    public void close() throws Exception {
        if (!isOpen) {
            return;
        }
        isOpen = false;
        tx = null;
    }

    public Variables variables() {
        return null;
    }

    public CatalogConfiguration configuration() {
        return configuration;
    }

    class GraphTransaction extends AbstractThreadLocalTransaction {

        public GraphTransaction() {
            super(CatalogGraph.this);
        }

        @Override
        protected void doOpen() {
            startNewTx();
        }

        @Override
        protected void doCommit() throws TransactionException {
            getAutoStartTx().commit();
        }

        @Override
        protected void doRollback() throws TransactionException {
            getAutoStartTx().rollback();
        }

        @Override
        public boolean isOpen() {
            if (tx == null) {
                return false;
            }
            return tx.get() != null && tx.get().isOpen();
        }
    }
}
