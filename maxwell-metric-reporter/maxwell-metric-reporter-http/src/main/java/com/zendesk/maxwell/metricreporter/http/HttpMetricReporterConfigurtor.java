package com.zendesk.maxwell.metricreporter.http;

import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.metricreporter.core.MetricReporterConfiguration;
import com.zendesk.maxwell.metricreporter.core.MetricReporterConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class HttpMetricReporterConfigurtor implements MetricReporterConfigurator {
    private static final String CONFIGURATION_OPTION_HTTP_PORT = "http_port";
    private static final String CONFIGURATION_OPTION_HTTP_BIND_ADDRESS = "http_bind_address";
    private static final String CONFIGURATION_OPTION_HTTP_PATH_PREFIX = "http_path_prefix";
    private static final String CONFIGURATION_OPTION_METRICS_HTTP_PORT = "metrics_http_port";
    private static final String CONFIGURATION_OPTION_HTTP_DIAGNOSTIC = "http_diagnostic";
    private static final String CONFIGURATION_OPTION_HTTP_DIAGNOSTIC_TIMEOUT = "http_diagnostic_timeout";

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpMetricReporterConfigurtor.class);

    private final HttpMetricReporter reporter;
    private final ConfigurationSupport configurationSupport;

    @Autowired
    public HttpMetricReporterConfigurtor(HttpMetricReporter reporter, ConfigurationSupport configurationSupport) {
        this.reporter = reporter;
        this.configurationSupport = configurationSupport;
    }

    @Override
    public String getIdentifier() {
        return "http";
    }

    @Override
    public void configureCommandLineOptions(CommandLineOptionParserContext context) {
        context.addOptionWithRequiredArgument( CONFIGURATION_OPTION_METRICS_HTTP_PORT, "[deprecated]" );
        context.addOptionWithRequiredArgument( CONFIGURATION_OPTION_HTTP_PORT, "the port the server will bind to when http reporting is configured" );
        context.addOptionWithRequiredArgument( CONFIGURATION_OPTION_HTTP_PATH_PREFIX, "the http path prefix when metrics_type includes http or diagnostic is enabled, default /" );
        context.addOptionWithRequiredArgument( CONFIGURATION_OPTION_HTTP_BIND_ADDRESS, "the ip address the server will bind to when http reporting is configured" );
        context.addOptionWithRequiredArgument( CONFIGURATION_OPTION_HTTP_DIAGNOSTIC, "enable http diagnostic endpoint: true|false. default: false" );
        context.addOptionWithRequiredArgument( CONFIGURATION_OPTION_HTTP_DIAGNOSTIC_TIMEOUT, "the http diagnostic response timeout in ms when http_diagnostic=true. default: 10000" );
    }

    @Override
    public Optional<MetricReporterConfiguration> parseConfiguration(Properties configurationValues) {
        HttpMetricReporterConfiguration config = new HttpMetricReporterConfiguration();

        // TODO remove metrics_http_port support once hitting v1.11.x
        String metricsHttpPort = configurationSupport.fetchOption(CONFIGURATION_OPTION_METRICS_HTTP_PORT, configurationValues, null);
        if (metricsHttpPort != null) {
            LOGGER.warn("metrics_http_port is deprecated, please use http_port");
            config.setHttpPort(Integer.parseInt(metricsHttpPort));
        } else {
            config.setHttpPort(configurationSupport.fetchIntegerOption(CONFIGURATION_OPTION_HTTP_PORT, configurationValues, HttpMetricReporterConfiguration.DEFAULT_HTTP_PORT));
        }
        config.setHttpBindAddress(configurationSupport.fetchOption(CONFIGURATION_OPTION_HTTP_BIND_ADDRESS, configurationValues, null));
        config.setHttpPathPrefix(configurationSupport.fetchOption(CONFIGURATION_OPTION_HTTP_PATH_PREFIX, configurationValues, HttpMetricReporterConfiguration.DEFAULT_HTTP_PATH_PREFIX));

        if (!config.getHttpPathPrefix().startsWith("/")) {
            config.setHttpPathPrefix("/" + config.getHttpPathPrefix());
        }

        config.setDiagnoticEnabled(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_HTTP_DIAGNOSTIC, configurationValues, HttpMetricReporterConfiguration.DEFAULT_DIAGNOSTIC_HTTP));
        config.setDiagnoticTimeout(configurationSupport.fetchLongOption(CONFIGURATION_OPTION_HTTP_DIAGNOSTIC_TIMEOUT, configurationValues, HttpMetricReporterConfiguration.DEFAULT_DIAGNOSTIC_HTTP_TIMEOUT));

        return Optional.of(config);
    }

    @Override
    public boolean isConfigured(Properties configurationOptions) {
        final String metricTypes = configurationOptions.getProperty(MaxwellConfig.CONFIGURATION_OPTION_METRICS_TYPE);
        final String diagnosticsEnabled = configurationOptions.getProperty(CONFIGURATION_OPTION_HTTP_DIAGNOSTIC);
        return getIdentifier().toLowerCase().equalsIgnoreCase(metricTypes) || Boolean.TRUE.toString().equalsIgnoreCase(diagnosticsEnabled);
    }

    @Override
    public void enableReporter(MetricReporterConfiguration configuration) {
        reporter.start((HttpMetricReporterConfiguration)configuration);
    }
}
