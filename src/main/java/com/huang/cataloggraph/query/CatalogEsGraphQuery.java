package com.huang.cataloggraph.query;

import com.huang.cataloggraph.config.GraphConfiguration;
import com.huang.cataloggraph.structure.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;

/**
 *
 **/
public class CatalogEsGraphQuery implements CatalogGraphQuery {
    private CatalogEsTransaction tx;

    SearchSourceBuilder builder;

    private final int batchSize = 10000;

    private int limit = Integer.MAX_VALUE;

    private int offset = 0;

    public CatalogEsGraphQuery(CatalogEsTransaction tx) {
        this.tx = tx;
        this.builder = new SearchSourceBuilder();
    }

    @Override
    public CatalogGraphQuery has(String propertyKey) {
        ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery(propertyKey);
        addQueryBuilder(existsQueryBuilder);
        return this;
    }

    @Override
    public CatalogGraphQuery has(String propertyKey, Object value) {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(propertyKey, value);
        addQueryBuilder(termQueryBuilder);
        return this;
    }

    @Override
    public CatalogGraphQuery has(String propertyKey, QueryOperator operator, Object value) {
        QueryBuilder queryBuilder = null;
        if (operator instanceof ComparisionOperator) {
            switch ((ComparisionOperator) operator) {
                case GREATER_THAN:
                    queryBuilder = QueryBuilders.rangeQuery(propertyKey).gt(value);
                    break;
                case GREATER_THAN_EQUAL:
                    queryBuilder = QueryBuilders.rangeQuery(propertyKey).gte(value);
                    break;
                case LESS_THAN:
                    queryBuilder = QueryBuilders.rangeQuery(propertyKey).lt(value);
                    break;
                case LESS_THAN_EQUAL:
                    queryBuilder = QueryBuilders.rangeQuery(propertyKey).lte(value);
                    break;
                case EQUAL:
                    queryBuilder = QueryBuilders.termQuery(propertyKey, value);
                    break;
                case NOT_EQUAL:
                    queryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(propertyKey, value));
                    break;
            }
        } else if (operator instanceof MatchingOperator) {
            switch ((MatchingOperator) operator) {
                case MATCH:
                    queryBuilder = QueryBuilders.matchQuery(propertyKey, value);
                    break;
                case PREFIX:
                    queryBuilder = QueryBuilders.prefixQuery(propertyKey, value.toString());
                    break;
                case REGEX:
                    queryBuilder = QueryBuilders.regexpQuery(propertyKey, value.toString());
            }
        }

        if (queryBuilder != null) {
            addQueryBuilder(queryBuilder);
        }
        return this;
    }

    @Override
    public CatalogGraphQuery in(String propertyKey, Collection<?> values) {
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery(propertyKey, values);
        addQueryBuilder(termsQueryBuilder);
        return this;
    }

    @Override
    public CatalogGraphQuery or(List<CatalogGraphQuery> childQueries) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        childQueries.forEach(q -> boolQueryBuilder.should(((CatalogEsGraphQuery) q).builder.query()));
        addQueryBuilder(boolQueryBuilder);
        return this;
    }

    @Override
    public CatalogGraphQuery limit(int limit) {
        this.limit = limit;
        builder.size(limit);
        return this;
    }

    @Override
    public CatalogGraphQuery offset(int offset) {
        this.offset = offset;
        builder.from(offset);
        return this;
    }

    @Override
    public CatalogQueryResult<CatalogVertex> vertices() {
        RestHighLevelClient client = tx.getGraph().getClient().getClient();

        SearchRequest request = new SearchRequest(GraphConfiguration.getVertex(tx.getGraph().configuration()));
        request.source(builder.trackTotalHits(true));

        if (limit == Integer.MAX_VALUE) {
            builder.from(0).size(batchSize);
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            request.scroll(scroll);
        }

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            CatalogQueryResult<CatalogVertex> res = new CatalogQueryResult<>();
            res.setCount(response.getHits().getTotalHits().value);

            if (limit == Integer.MAX_VALUE) {
                ElasticSearchScroll searchScroll = new ElasticSearchScroll(client, response, batchSize);

                res.setStream(StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(searchScroll, Spliterator.ORDERED), false)
                            .skip(offset)
                            .map(d -> GraphObjectFactory.createVertex(tx, d))
                );
            } else {
                res.setStream(Arrays.stream(response.getHits().getHits())
                    .map(hit -> new Document(hit.getId(), hit.getSourceAsMap()))
                    .map(d -> GraphObjectFactory.createVertex(tx, d))
                );
            }
            return res;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CatalogQueryResult<CatalogEdge> edges() {
        RestHighLevelClient client = tx.getGraph().getClient().getClient();

        SearchRequest request = new SearchRequest(GraphConfiguration.getEdge(tx.getGraph().configuration()));
        request.source(builder.trackTotalHits(true));

        if (limit == Integer.MAX_VALUE) {
            builder.from(0).size(batchSize);
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            request.scroll(scroll);
        }

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            CatalogQueryResult<CatalogEdge> res = new CatalogQueryResult<>();
            res.setCount(response.getHits().getTotalHits().value);

            if (limit == Integer.MAX_VALUE) {
                ElasticSearchScroll searchScroll = new ElasticSearchScroll(client, response, batchSize);

                res.setStream(StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(searchScroll, Spliterator.ORDERED), false)
                        .skip(offset)
                        .map(d -> GraphObjectFactory.createEdge(tx, d))
                );
            } else {
                res.setStream(Arrays.stream(response.getHits().getHits())
                        .map(hit -> new Document(hit.getId(), hit.getSourceAsMap()))
                        .map(d -> GraphObjectFactory.createEdge(tx, d))
                );
            }
            return res;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void addQueryBuilder(QueryBuilder queryBuilder) {
        if (builder.query() == null) {
            if (queryBuilder instanceof BoolQueryBuilder) {
                builder.query(QueryBuilders.boolQuery().must(queryBuilder));
            } else {
                builder.query(queryBuilder);
            }
        } else {
            if (!(builder.query() instanceof BoolQueryBuilder)) {
                builder.query(QueryBuilders.boolQuery().must(builder.query()));
            }

            ((BoolQueryBuilder) builder.query()).must(queryBuilder);
        }
    }
}
