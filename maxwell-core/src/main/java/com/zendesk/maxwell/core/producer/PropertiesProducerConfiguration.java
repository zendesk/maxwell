package com.zendesk.maxwell.core.producer;

import java.util.Properties;

public class PropertiesProducerConfiguration implements ProducerConfiguration {

	private final Properties properties;

	public PropertiesProducerConfiguration(Properties properties) {
		this.properties = properties;
	}

	public Properties getProperties() {
		return properties;
	}
}