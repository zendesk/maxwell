package com.zendesk.maxwell.core.producer.impl.file;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.ConfigurationSupport;
import com.zendesk.maxwell.core.config.ExtensionConfiguration;
import com.zendesk.maxwell.core.config.ExtensionConfigurator;
import com.zendesk.maxwell.core.config.ExtensionType;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerInstantiationException;
import joptsimple.OptionSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

@Service
public class FileProducerConfigurator implements ExtensionConfigurator<Producer> {
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
	public ExtensionType getExtensionType() {
		return ExtensionType.PRODUCER;
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(OptionSet commandLineArguments, Properties configurationValues) {

		String outputFile = configurationSupport.fetchOption("output_file", commandLineArguments, configurationValues, null);
		return Optional.of(new FileProducerConfiguration(outputFile));
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		try {
			FileProducerConfiguration config = context.getConfig().getProducerConfigOrThrowExceptionWhenNotDefined();
			return new FileProducer(context, config.getOutputFile());
		} catch (IOException e) {
			throw new ProducerInstantiationException(e);
		}
	}
}
