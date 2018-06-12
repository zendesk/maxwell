package com.zendesk.maxwell.metricreporter.core;

import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.config.MaxwellConfig;

import java.util.Optional;
import java.util.Properties;

public interface MetricReporterConfigurator {

    String getIdentifier();

    default void configureCommandLineOptions(CommandLineOptionParserContext context) {
    }

    default Optional<MetricReporterConfiguration> parseConfiguration(Properties configurationValues) {
        return Optional.empty();
    }

    default boolean isConfigured(Properties configurationOptions){
        return getIdentifier().toLowerCase().equalsIgnoreCase(configurationOptions.getProperty(MaxwellConfig.CONFIGURATION_OPTION_METRICS_TYPE));
    }

    void enableReporter(MetricReporterConfiguration configuration);
}
