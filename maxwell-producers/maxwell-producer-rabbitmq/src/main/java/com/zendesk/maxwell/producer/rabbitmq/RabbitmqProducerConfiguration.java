package com.zendesk.maxwell.producer.rabbitmq;

import com.zendesk.maxwell.core.producer.ProducerConfiguration;

public class RabbitmqProducerConfiguration implements ProducerConfiguration {
	private String rabbitmqUser;
	private String rabbitmqPass;
	private String rabbitmqHost;
	private int rabbitmqPort;
	private String rabbitmqVirtualHost;
	private String rabbitmqExchange;
	private String rabbitmqExchangeType;
	private boolean rabbitMqExchangeDurable;
	private boolean rabbitMqExchangeAutoDelete;
	private String rabbitmqRoutingKeyTemplate;
	private boolean rabbitmqMessagePersistent;
	private boolean rabbitmqDeclareExchange;

	public String getRabbitmqUser() {
		return rabbitmqUser;
	}

	public void setRabbitmqUser(String rabbitmqUser) {
		this.rabbitmqUser = rabbitmqUser;
	}

	public String getRabbitmqPass() {
		return rabbitmqPass;
	}

	public void setRabbitmqPass(String rabbitmqPass) {
		this.rabbitmqPass = rabbitmqPass;
	}

	public String getRabbitmqHost() {
		return rabbitmqHost;
	}

	public void setRabbitmqHost(String rabbitmqHost) {
		this.rabbitmqHost = rabbitmqHost;
	}

	public int getRabbitmqPort() {
		return rabbitmqPort;
	}

	public void setRabbitmqPort(int rabbitmqPort) {
		this.rabbitmqPort = rabbitmqPort;
	}

	public String getRabbitmqVirtualHost() {
		return rabbitmqVirtualHost;
	}

	public void setRabbitmqVirtualHost(String rabbitmqVirtualHost) {
		this.rabbitmqVirtualHost = rabbitmqVirtualHost;
	}

	public String getRabbitmqExchange() {
		return rabbitmqExchange;
	}

	public void setRabbitmqExchange(String rabbitmqExchange) {
		this.rabbitmqExchange = rabbitmqExchange;
	}

	public String getRabbitmqExchangeType() {
		return rabbitmqExchangeType;
	}

	public void setRabbitmqExchangeType(String rabbitmqExchangeType) {
		this.rabbitmqExchangeType = rabbitmqExchangeType;
	}

	public boolean isRabbitMqExchangeDurable() {
		return rabbitMqExchangeDurable;
	}

	public void setRabbitMqExchangeDurable(boolean rabbitMqExchangeDurable) {
		this.rabbitMqExchangeDurable = rabbitMqExchangeDurable;
	}

	public boolean isRabbitMqExchangeAutoDelete() {
		return rabbitMqExchangeAutoDelete;
	}

	public void setRabbitMqExchangeAutoDelete(boolean rabbitMqExchangeAutoDelete) {
		this.rabbitMqExchangeAutoDelete = rabbitMqExchangeAutoDelete;
	}

	public String getRabbitmqRoutingKeyTemplate() {
		return rabbitmqRoutingKeyTemplate;
	}

	public void setRabbitmqRoutingKeyTemplate(String rabbitmqRoutingKeyTemplate) {
		this.rabbitmqRoutingKeyTemplate = rabbitmqRoutingKeyTemplate;
	}

	public boolean isRabbitmqMessagePersistent() {
		return rabbitmqMessagePersistent;
	}

	public void setRabbitmqMessagePersistent(boolean rabbitmqMessagePersistent) {
		this.rabbitmqMessagePersistent = rabbitmqMessagePersistent;
	}

	public boolean isRabbitmqDeclareExchange() {
		return rabbitmqDeclareExchange;
	}

	public void setRabbitmqDeclareExchange(boolean rabbitmqDeclareExchange) {
		this.rabbitmqDeclareExchange = rabbitmqDeclareExchange;
	}
}
