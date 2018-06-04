package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.MaxwellContext;
import joptsimple.OptionSet;

import java.util.Optional;
import java.util.Properties;

public interface ExtensionConfigurator<E extends Extension> {

	String getExtensionIdentifier();

	ExtensionType getExtensionType();

	default void configureCommandLineOptions(CommandLineOptionParserContext context) {
	}

	default Optional<ExtensionConfiguration> parseConfiguration(OptionSet commandLineArguments, Properties configurationValues) {
		return Optional.empty();
	}

	E createInstance(MaxwellContext context);

}
