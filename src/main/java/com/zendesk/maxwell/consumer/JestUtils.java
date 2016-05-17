package com.zendesk.maxwell.consumer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.core.Update;

public class JestUtils {
	static final Logger LOGGER = LoggerFactory.getLogger(JestUtils.class);
	static JestClient client = null;
	
	public static JestClient getJestClient(){
		if(client != null){
			return client;
		}
		HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://localhost:9200") 
		        .discoveryEnabled(true)
		        .discoveryFrequency(10l, TimeUnit.SECONDS)
		        .readTimeout(10000)
		        .multiThreaded(true)
		        .build();
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(clientConfig);
		JestClient jClient = factory.getObject();
		return jClient;
	}
	
	public static void jestUpdate(String indx,String type,String id,String data){
		
		JestClient jestClient = getJestClient();
		Update update = new Update.Builder(data).index(indx).type(type).id(id).refresh(true).build();
		DocumentResult result = null;
		try {
			result = jestClient.execute(update);
		} catch (IOException e) {
			LOGGER.error("Error occured while Updating document with id "+id+" in index "+indx+"\\"+type);
			e.printStackTrace();
		}
		int response = result.getResponseCode();
		if(response != 200){
			LOGGER.info("Document could not be indexed ::"+data);
			LOGGER.info(result.getErrorMessage());
		}else{
			LOGGER.info("Document successfully indexed ::"+data);
		}
		//jestClient.shutdownClient();
	}
	
	public static void jestIndex(String indx,String type,String id,String data){

		JestClient jestClient = getJestClient();
		Index index = new Index.Builder(data).index(indx).type(type).id(id).refresh(true).build();
		DocumentResult result = null;
		try {
			result = jestClient.execute(index);
		} catch (IOException e) {
			LOGGER.error("Error occured while Indexing document with id "+id+" in index "+indx+"\\"+type);
			e.printStackTrace();
		}
		int response = result.getResponseCode();
		if(response != 200){
			LOGGER.info("Document could not be indexed ::"+data);
			LOGGER.info(result.getErrorMessage());
		}else{
			LOGGER.info("Document successfully indexed ::"+data);
		}
		//jestClient.shutdownClient();
	}

}
