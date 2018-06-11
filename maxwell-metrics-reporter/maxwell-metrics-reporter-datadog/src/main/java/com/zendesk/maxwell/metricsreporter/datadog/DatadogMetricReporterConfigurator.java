package com.zendesk.maxwell.metricsreporter.datadog;

import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.monitoring.MetricReporterConfiguration;
import com.zendesk.maxwell.api.monitoring.MetricReporterConfigurator;
import com.zendesk.maxwell.api.monitoring.Metrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

import static com.zendesk.maxwell.metricsreporter.datadog.DatadogMetricReporterConfiguration.*;

@Service
public class DatadogMetricReporterConfigurator implements MetricReporterConfigurator {
    private final DatadogMetricReporter reporter;
    private final ConfigurationSupport configurationSupport;

    @Autowired
    public DatadogMetricReporterConfigurator(DatadogMetricReporter reporter, ConfigurationSupport configurationSupport) {
        this.reporter = reporter;
        this.configurationSupport = configurationSupport;
    }

    @Override
    public String getIdentifier() {
        return Metrics.REPORTING_TYPE_DATADOG;
    }

    @Override
    public void configureCommandLineOptions(CommandLineOptionParserContext context) {
        context.addOptionWithRequiredArgument( "metrics_datadog_type", "when metrics_type includes datadog this is the way metrics will be reported, one of udp|http" );
        context.addOptionWithRequiredArgument( "metrics_datadog_tags", "datadog tags that should be supplied, e.g. tag1:value1,tag2:value2" );
        context.addOptionWithRequiredArgument( "metrics_datadog_interval", "the frequency metrics are pushed to datadog, in seconds" );
        context.addOptionWithRequiredArgument( "metrics_datadog_apikey", "the datadog api key to use when metrics_datadog_type = http" );
        context.addOptionWithRequiredArgument( "metrics_datadog_host", "the host to publish metrics to when metrics_datadog_type = udp" );
        context.addOptionWithRequiredArgument( "metrics_datadog_port", "the port to publish metrics to when metrics_datadog_type = udp" );
    }

    @Override
    public Optional<MetricReporterConfiguration> parseConfiguration(Properties configurationValues) {
        DatadogMetricReporterConfiguration config = new DatadogMetricReporterConfiguration();
        config.setType(configurationSupport.fetchOption("metrics_datadog_type", configurationValues, DEFAULT_METRICS_DATADOG_TYPE));
        config.setTags(configurationSupport.fetchOption("metrics_datadog_tags", configurationValues, DEFAULT_METRICS_DATADOG_TAGS));
        config.setApiKey(configurationSupport.fetchOption("metrics_datadog_apikey", configurationValues, DEFAULT_METRICS_DATADOG_APIKEY));
        config.setHost(configurationSupport.fetchOption("metrics_datadog_host", configurationValues, DEFAULT_METRICS_DATADOG_HOST));
        config.setPort(configurationSupport.fetchIntegerOption("metrics_datadog_port", configurationValues, DEFAULT_METRICS_DATADOG_PORT));
        config.setInterval(configurationSupport.fetchLongOption("metrics_datadog_interval", configurationValues, DEFAULT_METRICS_DATADOG_INTERVAL));
        return Optional.empty();
    }

    @Override
    public void enableReporter(MetricReporterConfiguration configuration) {
        reporter.start((DatadogMetricReporterConfiguration)configuration);
    }
}
