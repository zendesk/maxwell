package com.zendesk.maxwell.producer.redis;

import com.zendesk.maxwell.core.producer.ProducerConfiguration;

public class RedisProducerConfiguration implements ProducerConfiguration {
	public String host;
	public int port;
	public String auth;
	public int database;
	public String pubChannel;
	public String listKey;
	public String type;
}
