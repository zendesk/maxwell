package com.zendesk.maxwell.core.producer.impl.file;

import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.producer.ProducerConfiguration;
import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class FileProducerConfigurator implements ProducerConfigurator {
	private final ConfigurationSupport configurationSupport;

	@Autowired
	public FileProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getIdentifier() {
		return "file";
	}

	@Override
	public Optional<ProducerConfiguration> parseConfiguration(Properties configurationValues) {

		String outputFile = configurationSupport.fetchOption("output_file", configurationValues, null);
		return Optional.of(new FileProducerConfiguration(outputFile));
	}

	@Override
	public Class<? extends ProducerFactory> getFactory() {
		return FileProducerFactory.class;
	}
}
