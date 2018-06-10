package com.zendesk.maxwell.metricsreporter.core;

import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.monitoring.MetricReporterConfiguration;
import com.zendesk.maxwell.api.monitoring.MetricReporterConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

@Service
public class MetricsReporterInitialization {

    private final List<MetricReporterConfigurator> metricReporterConfigurators;

    @Autowired
    public MetricsReporterInitialization(List<MetricReporterConfigurator> metricReporterConfigurators) {
        this.metricReporterConfigurators = metricReporterConfigurators;
    }

    public void setup(Properties configurationSettings){
        final String configuredMetricsReporters = configurationSettings.getProperty(MaxwellConfig.CONFIGURATION_OPTION_METRICS_TYPE);
        if(configuredMetricsReporters != null){
            metricReporterConfigurators.stream().filter(c -> isConfigured(c, configuredMetricsReporters)).forEach(c -> enableReporter(c, configurationSettings));
        }
    }

    private boolean isConfigured(MetricReporterConfigurator configurator, String configuredReporterTypes){
        return configuredReporterTypes.toLowerCase().contains(configurator.getIdentifier().toLowerCase());
    }

    private void enableReporter(MetricReporterConfigurator configurator, Properties configurationSettings){
        MetricReporterConfiguration configuration = configurator.parseConfiguration(configurationSettings).orElse(MetricReporterConfiguration.EMTPY);
        configurator.enableReporter(configuration);
    }

}
