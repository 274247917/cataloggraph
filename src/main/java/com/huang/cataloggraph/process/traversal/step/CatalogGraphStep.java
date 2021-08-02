package com.huang.cataloggraph.process.traversal.step;

import com.huang.cataloggraph.query.CatalogGraphQuery;
import com.huang.cataloggraph.structure.CatalogGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;

/**
 *
 **/
public class CatalogGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder, AutoCloseable {
    private final List<HasContainer> hasContainers = new ArrayList<>();

    public CatalogGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(),
                originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);

        this.setIteratorSupplier(() -> {
            CatalogGraph graph = (CatalogGraph) this.getTraversal().getGraph().get();

            CatalogGraphQuery query = buildQuery(graph);
            if (Vertex.class.isAssignableFrom(this.returnClass)) {
                return (Iterator<E>) query.vertices().getStream().iterator();
            } else {
                return (Iterator<E>) query.edges().getStream().iterator();
            }
        });
    }

    private CatalogGraphQuery buildQuery(CatalogGraph graph) {
        CatalogGraphQuery query = graph.query();
        if (!hasContainers.isEmpty()) {
            hasContainers.forEach(c -> {
                BiPredicate<?, ?> predicate = c.getPredicate().getBiPredicate();
                if (predicate instanceof Compare) {
                    query.has(c.getKey(), CatalogGraphPredicateUtil.convert((Compare) predicate), c.getValue());
                }
            });
        }
        return query;
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }
}
