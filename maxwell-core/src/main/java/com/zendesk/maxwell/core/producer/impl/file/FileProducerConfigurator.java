package com.zendesk.maxwell.core.producer.impl.file;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import com.zendesk.maxwell.api.producer.ProducerInstantiationException;
import com.zendesk.maxwell.core.config.ConfigurationSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

@Service
public class FileProducerConfigurator implements ProducerConfigurator<FileProducerConfiguration> {
	private final ConfigurationSupport configurationSupport;

	@Autowired
	public FileProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getExtensionIdentifier() {
		return "file";
	}

	@Override
	public Optional<FileProducerConfiguration> parseConfiguration(Properties configurationValues) {

		String outputFile = configurationSupport.fetchOption("output_file", configurationValues, null);
		return Optional.of(new FileProducerConfiguration(outputFile));
	}

	@Override
	public Producer createInstance(MaxwellContext context, FileProducerConfiguration configuration) {
		try {
			return new FileProducer(context, configuration.getOutputFile());
		} catch (IOException e) {
			throw new ProducerInstantiationException(e);
		}
	}
}
