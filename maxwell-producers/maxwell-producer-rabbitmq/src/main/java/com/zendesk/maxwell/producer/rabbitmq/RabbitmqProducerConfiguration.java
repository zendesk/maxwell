package com.zendesk.maxwell.producer.rabbitmq;

import com.zendesk.maxwell.core.producer.ProducerConfiguration;

public class RabbitmqProducerConfiguration implements ProducerConfiguration {
	public String user;
	public String password;
	public String host;
	public int port;
	public String virtualHost;
	public String exchange;
	public String exchangeType;
	public boolean durableExchange;
	public boolean autoDeleteExchange;
	public String routingKeyTemplate;
	public boolean persistentMessages;
	public boolean declareExchange;
}
