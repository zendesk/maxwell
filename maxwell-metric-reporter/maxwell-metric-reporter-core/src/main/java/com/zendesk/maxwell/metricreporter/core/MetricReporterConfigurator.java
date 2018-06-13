package com.zendesk.maxwell.metricreporter.core;

import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;

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
        return getIdentifier().toLowerCase().equalsIgnoreCase(configurationOptions.getProperty("metrics_type"));
    }

    void enableReporter(MetricReporterConfiguration configuration);
}
