package com.huang.cataloggraph.query;

import java.util.stream.Stream;

/**
 *
 **/
public class CatalogQueryResult<T> {
    private long count;
    private Stream<T> stream;

    public CatalogQueryResult() {
    }

    public CatalogQueryResult(long count, Stream<T> stream) {
        this.count = count;
        this.stream = stream;
    }

    public long getCount() {
        return count;
    }

    public Stream<T> getStream() {
        return stream;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setStream(Stream<T> stream) {
        this.stream = stream;
    }
}
