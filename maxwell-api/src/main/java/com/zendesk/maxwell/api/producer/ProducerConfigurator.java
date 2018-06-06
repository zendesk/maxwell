package com.zendesk.maxwell.api.producer;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;

import java.util.Optional;
import java.util.Properties;

public interface ProducerConfigurator<C extends ProducerConfiguration> {

	String getExtensionIdentifier();

	default void configureCommandLineOptions(CommandLineOptionParserContext context) {
	}

	default ProducerContext createProducerContext(MaxwellContext context, Properties settings){
		Optional<C> optionalConfiguration = parseConfiguration(settings).map(c -> {
			c.mergeWith(context.getConfig());
			c.validate();
			return c;
		});
		Producer producer = createInstance(context, optionalConfiguration.orElse(null));

		ProducerConfiguration configuration = optionalConfiguration.map(c -> (ProducerConfiguration)c).orElse(new PropertiesProducerConfiguration(new Properties()));
		return new ProducerContext(configuration, producer);
	}

	default Optional<C> parseConfiguration(Properties configurationValues) {
		return Optional.empty();
	}
	Producer createInstance(MaxwellContext context, C configuration);

}
