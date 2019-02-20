package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);
    private static final long SIZE = 104858;
    
    private static BulkRequest request = new BulkRequest();
    private static RestHighLevelClient client = ElasticSearchRepository.createClient();

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        
        
        String inputFilePath = args[0];
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                    .forEach(line -> {

                    	 if(count.get() == 0) {
                             count.getAndIncrement();
                         } 
                         else {
                             String jsonString = "{ \"name\": \"" + line.replace("\"", "") + "\" }";
                             request.add(
                                 new IndexRequest("actors")
                                     .id(Integer.toString(count.getAndIncrement()))
                                     .type("_doc")
                                     .source(jsonString, XContentType.JSON)
                             );
                             if(count.get() % SIZE == 0) {
                            	 
                             	try {
                                     BulkResponse bulkResponse = client.bulk(request);
                                     if(bulkResponse.hasFailures()) {
                                         System.out.println(bulkResponse.buildFailureMessage());
                                     }
                                 } catch (Exception e) {
                                     System.out.println(e.getMessage());
                                 } finally {
                                     request = new BulkRequest();
                                 }
                             }
                         }
                    });
        }

        System.out.println("Inserted total of " + count.get() + " actors");

        client.close();
    }
    	
}
