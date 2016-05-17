package com.zendesk.maxwell.consumer;

public class ElasticConsumerTest {
	
	public static void main(String[] args){
		KafkaElasticConsumer consumer = new KafkaElasticConsumer();
		Thread consume = new Thread(consumer);
		consume.start();
		MysqlWriter writer = new MysqlWriter();
		Thread create = new Thread(writer);
		create.start();
	}
}
