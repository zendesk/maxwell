package com.zendesk.maxwell.api.producer;

public class ProducerContext {
	private final ProducerConfiguration configuration;
	private final Producer producer;

	public ProducerContext(ProducerConfiguration configuration, Producer producer) {
		this.configuration = configuration;
		this.producer = producer;
	}

	public <T extends ProducerConfiguration> T getConfiguration(){
		return (T)configuration;
	}

	public Producer getProducer() {
		return producer;
	}
}
