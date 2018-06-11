package com.zendesk.maxwell.metricsreporter.core.slf4j;

import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.monitoring.MetricReporterConfiguration;
import com.zendesk.maxwell.api.monitoring.MetricReporterConfigurator;
import com.zendesk.maxwell.api.monitoring.Metrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class Slf4jMetricReporterConfigurator implements MetricReporterConfigurator {
    private final ConfigurationSupport configurationSupport;

    @Autowired
    public Slf4jMetricReporterConfigurator(ConfigurationSupport configurationSupport) {
        this.configurationSupport = configurationSupport;
    }

    @Override
    public String getIdentifier() {
        return Metrics.REPORTING_TYPE_SLF4J;
    }

    @Override
    public void configureCommandLineOptions(CommandLineOptionParserContext context) {
        context.addOptionWithRequiredArgument( "metrics_slf4j_interval", "the frequency metrics are emitted to the log, in seconds, when slf4j reporting is configured" );
    }

    @Override
    public Optional<MetricReporterConfiguration> parseConfiguration(Properties configurationValues) {
        final Long interval = configurationSupport.fetchLongOption("metrics_slf4j_interval", configurationValues, Slf4jMetricReporterConfiguration.DEFAULT_METRITCS_SLF4J_INTERVAL);
        return Optional.of(new Slf4jMetricReporterConfiguration(interval));
    }

    @Override
    public void enableReporter(MetricReporterConfiguration configuration, MetricRegistry metricRegistry) {
        Slf4jMetricReporterConfiguration config = (Slf4jMetricReporterConfiguration)configuration;
        Slf4jMetricReporter reporter = new Slf4jMetricReporter(config);
        reporter.start(metricRegistry);
    }
}
