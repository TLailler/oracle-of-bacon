package com.serli.oracle.of.bacon.repository;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.*;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticSearchRepository {

    private final RestHighLevelClient client;

    public ElasticSearchRepository() {
        client = createClient();

    }

    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
    }

    public List<String> getActorsSuggests(String searchQuery) throws IOException {

    	SearchRequest searchRequest = new SearchRequest("actors");
    	SuggestBuilder suggestBuilder = new SuggestBuilder();
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    	
    	
		SuggestionBuilder<CompletionSuggestionBuilder> completionSuggestionBuilder = 
				SuggestBuilders.completionSuggestion("suggest")
				.text(searchQuery)
				.size(10); 
		 	
    	suggestBuilder.addSuggestion("actors_completion", completionSuggestionBuilder); 
    	searchSourceBuilder.suggest(suggestBuilder);   	
    	searchRequest.source(searchSourceBuilder);
    	
    	SearchResponse searchResponse = client.search(searchRequest);	
    	CompletionSuggestion completionSuggestion = searchResponse.getSuggest().getSuggestion("actors_completion");
    	
    	List<String> suggestions = new ArrayList<>();
    	suggestions = completionSuggestion.getEntries()
    			.stream()
    			.flatMap(e -> e.getOptions()
    							.stream()
    							.map(o -> o.getHit()
    									.getSourceAsMap()
    									.get("name")
    									.toString()))
    			.collect(Collectors.toList());
    			
    	
        return suggestions;
    }
}
