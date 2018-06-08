package com.zendesk.maxwell.api.config;

import java.util.Optional;
import java.util.Properties;

public interface ModuleConfigurator<C extends ModuleConfiguration> {

    ModuleType getType();
    String getIdentifier();

    default void configureCommandLineOptions(CommandLineOptionParserContext context) {
    }

    default Optional<C> parseConfiguration(Properties configurationValues) {
        return Optional.empty();
    }
}
