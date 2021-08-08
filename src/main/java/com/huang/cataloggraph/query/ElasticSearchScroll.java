package com.huang.cataloggraph.query;

import com.huang.cataloggraph.structure.Document;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 *
 **/
public class ElasticSearchScroll implements Iterator<Document> {
    private final BlockingQueue<Document> queue;
    private final RestHighLevelClient client;
    private final int size;

    private boolean isFinished;
    private String scrollId;

    public ElasticSearchScroll(RestHighLevelClient client, SearchResponse response, int size) {
        this.queue = new LinkedBlockingQueue<>();
        this.client = client;
        this.size = size;
        this.scrollId = response.getScrollId();
        List<Document> documents = Arrays.stream(response.getHits().getHits())
                .map(hit -> new Document(hit.getId(), hit.getSourceAsMap()))
                .collect(Collectors.toList());
        queue.addAll(documents);
        this.isFinished = response.getHits().getTotalHits().value < size;
    }

    @Override
    public boolean hasNext() {
        try {
            if (!queue.isEmpty()) {
                return true;
            }
            if (isFinished) {
                return false;
            }
            SearchScrollRequest scroll = new SearchScrollRequest()
                    .scrollId(scrollId)
                    .scroll(TimeValue.timeValueMinutes(1L));

            SearchResponse response = client.scroll(scroll, RequestOptions.DEFAULT);
            List<Document> documents = Arrays.stream(response.getHits().getHits())
                    .map(hit -> new Document(hit.getId(), hit.getSourceAsMap()))
                    .collect(Collectors.toList());

            int count = response.getHits().getHits().length;
            isFinished = count < size;

            if (isFinished) {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            }
            return count > 0;
        } catch (final IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public Document next() {
        if (hasNext()) {
            return queue.remove();
        }
        throw new NoSuchElementException();
    }
}
