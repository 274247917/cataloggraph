package com.huang.cataloggraph.structure;

import com.huang.cataloggraph.config.CatalogConfiguration;
import com.huang.cataloggraph.config.GraphConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 *
 **/
public class ElasticSearchClient {
    private RestHighLevelClient client;

    private static final Pattern PATTERN = Pattern.compile("(\\d+)\\.\\d+\\.\\d+.*");

    private static final int ES_SUPPORT_VERSION = 7;

    public ElasticSearchClient(CatalogConfiguration configuration) {
        String endpoint = configuration.get(GraphConfiguration.INDEX_HOSTNAME);
        if (StringUtils.isEmpty(endpoint)) {
            throw new IllegalArgumentException("index.hostname is null");
        }

        client = new RestHighLevelClient(RestClient.builder(getEsHttpHost(endpoint)));
        try {
            MainResponse info = client.info(RequestOptions.DEFAULT);
            String number = info.getVersion().getNumber();

            Matcher matcher = PATTERN.matcher(number);
            if (!(matcher.find() && Integer.valueOf(matcher.group(1)) == ES_SUPPORT_VERSION)) {
                throw new IllegalArgumentException("Unsupported es version");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to get es version", e);
        }
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    private HttpHost[] getEsHttpHost(String endpoint) {
        List<HttpHost> httpHosts = Arrays.stream(endpoint.split(",")).map(a -> {
            String[] address = a.split(":");
            return new HttpHost(address[0], Integer.valueOf(address[1]));
        }).collect(Collectors.toList());
        return httpHosts.toArray(new HttpHost[0]);
    }
}
