package com.zendesk.maxwell.api.monitoring;

import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.config.ModuleConfigurator;
import com.zendesk.maxwell.api.config.ModuleType;

import java.util.Properties;

public interface MetricReporterConfigurator extends ModuleConfigurator<MetricReporterConfiguration> {

    @Override
    default ModuleType getType() {
        return ModuleType.METRIC_REPORTER;
    }

    default boolean isConfigured(Properties configurationOptions){
        return getIdentifier().toLowerCase().equalsIgnoreCase(configurationOptions.getProperty(MaxwellConfig.CONFIGURATION_OPTION_METRICS_TYPE));
    }

    void enableReporter(MetricReporterConfiguration configuration);
}
