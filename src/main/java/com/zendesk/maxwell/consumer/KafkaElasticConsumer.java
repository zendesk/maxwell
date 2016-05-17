package com.zendesk.maxwell.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;


public class KafkaElasticConsumer implements Runnable{
	static final Logger LOGGER = LoggerFactory.getLogger(JestUtils.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String,String> consumer = null;

    public void run() {
        ObjectMapper mapper = new ObjectMapper();
        try {
        	InputStream props;
        	int count =0;
			try {
				props = Resources.getResource("consumer.props").openStream();
	        	Properties properties = new Properties();
	            properties.load(props);
	            if (properties.getProperty("group.id") == null) {
	                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
	            }
	            consumer = new KafkaConsumer<>(properties);
	            consumer.subscribe(Arrays.asList("maxwell"));
			} catch (IOException e) {
				e.printStackTrace();
			}
            while (!closed.get()) {
            	ConsumerRecords<String, String> records = consumer.poll(10000);
                for (ConsumerRecord<String,String> record : records) {
            	JsonNode msg = null;
            	JsonNode dataNode = null;
            	String data = null;
				try {
					msg = mapper.readTree(record.value());
					dataNode = msg.get("data");
					data = "{ \"doc\": "+mapper.writeValueAsString(dataNode)+"}";
					//String dat = mapper.writeValueAsString(dataNode);
					//System.out.println(dat);
					//System.out.println(dataNode.toString());
				} catch (IOException e) {
					e.printStackTrace();
				} 
				StringBuilder sb = new StringBuilder();
				String eventType = msg.get("type").asText();
				System.out.println(data);
				String id = dataNode.get("emp_no").asText();
				switch(eventType){
				case "insert":
					// Index the record in Elasticsearch
					//JestUtils.jestIndex("employees","employee", id, data);
					HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://localhost:9200") 
			        .discoveryEnabled(true)
			        .discoveryFrequency(10l, TimeUnit.SECONDS)
			        .readTimeout(10000)
			        .multiThreaded(true)
			        .build();
					JestClientFactory factory = new JestClientFactory();
					factory.setHttpClientConfig(clientConfig);
					JestClient jestClient = factory.getObject();
					Index index = new Index.Builder(data).index("employees").type("employee").id(id).refresh(true).build();
			        /*Index asyncindex = new Index.Builder(data).index("employees").type("employee").id(id).refresh(true).build();
			        jestClient.executeAsync(asyncindex, new JestResultHandler() {
			            public void failed(Exception ex) {
			            	System.out.println("failed");
			            }
			 
			            public void completed(Object result) {
			            	System.out.println("completed");
			            }
			        });*/
					DocumentResult result = null;
					try {
						result = jestClient.execute(index);
					} catch (IOException e) {
						LOGGER.error("Error occured while Indexing document with id "+id+" in index "+"employees"+"\\"+"employee");
						e.printStackTrace();
					}
					int response = result.getResponseCode();
					if(response != 200){
						LOGGER.info("Document could not be indexed ::"+data);
					}else{
						LOGGER.info("Document successfully indexed ::"+data);
					}
				case "update":
					// Update the record in Elasticsearch
					JestUtils.jestUpdate("employees","employee", id, data);
				}
                }            
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
	
}
