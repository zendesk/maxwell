package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.producer.Producer;

import java.util.Optional;
import java.util.Properties;

public interface ProducerConfigurator {

	String getIdentifier();

	default void configureCommandLineOptions(CommandLineOptionParserContext context) {
	}

	default Optional<ProducerConfiguration> parseConfiguration(Properties configurationValues) {
		return Optional.empty();
	}

	Producer configure(MaxwellContext maxwellContext, ProducerConfiguration configuration);

}
