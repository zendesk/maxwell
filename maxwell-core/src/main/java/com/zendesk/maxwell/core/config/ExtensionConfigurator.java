package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.MaxwellContext;

import java.util.Optional;
import java.util.Properties;

public interface ExtensionConfigurator<E extends Extension> {

	String getExtensionIdentifier();

	ExtensionType getExtensionType();

	default String getCommandLineArgumentPrefix() {
		return getExtensionIdentifier();
	}

	default String getConfigurationParameterPrefix() {
		return getExtensionIdentifier();
	}

	default void configureCommandLineOptions(CommandLineOptionParserContext context) {
	}

	default Optional<ExtensionConfiguration> parseConfiguration(Properties commandLineArguments, Properties configurationValues) {
		return Optional.empty();
	}

	E createInstance(MaxwellContext context);

}
