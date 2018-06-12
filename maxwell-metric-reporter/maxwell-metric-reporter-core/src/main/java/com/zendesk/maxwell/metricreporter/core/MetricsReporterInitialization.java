package com.zendesk.maxwell.metricreporter.core;

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
        metricReporterConfigurators.stream().filter(c -> c.isConfigured(configurationSettings)).forEach(c -> enableReporter(c, configurationSettings));
    }

    private void enableReporter(MetricReporterConfigurator configurator, Properties configurationSettings){
        MetricReporterConfiguration configuration = configurator.parseConfiguration(configurationSettings).orElse(MetricReporterConfiguration.EMTPY);
        configuration.validate();
        configurator.enableReporter(configuration);
    }

}
