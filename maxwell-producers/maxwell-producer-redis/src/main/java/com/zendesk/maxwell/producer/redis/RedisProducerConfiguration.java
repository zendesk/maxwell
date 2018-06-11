package com.zendesk.maxwell.producer.redis;

import com.zendesk.maxwell.api.producer.ProducerConfiguration;

public class RedisProducerConfiguration implements ProducerConfiguration {
	private String redisHost;
	private int redisPort;
	private String redisAuth;
	private int redisDatabase;
	private String redisPubChannel;
	private String redisListKey;
	private String redisType;

	public String getRedisHost() {
		return redisHost;
	}

	public void setRedisHost(String redisHost) {
		this.redisHost = redisHost;
	}

	public int getRedisPort() {
		return redisPort;
	}

	public void setRedisPort(int redisPort) {
		this.redisPort = redisPort;
	}

	public String getRedisAuth() {
		return redisAuth;
	}

	public void setRedisAuth(String redisAuth) {
		this.redisAuth = redisAuth;
	}

	public int getRedisDatabase() {
		return redisDatabase;
	}

	public void setRedisDatabase(int redisDatabase) {
		this.redisDatabase = redisDatabase;
	}

	public String getRedisPubChannel() {
		return redisPubChannel;
	}

	public void setRedisPubChannel(String redisPubChannel) {
		this.redisPubChannel = redisPubChannel;
	}

	public String getRedisListKey() {
		return redisListKey;
	}

	public void setRedisListKey(String redisListKey) {
		this.redisListKey = redisListKey;
	}

	public String getRedisType() {
		return redisType;
	}

	public void setRedisType(String redisType) {
		this.redisType = redisType;
	}
}
