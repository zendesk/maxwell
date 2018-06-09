package com.zendesk.maxwell.api.monitoring;

import com.zendesk.maxwell.api.config.ModuleConfigurator;
import com.zendesk.maxwell.api.config.ModuleType;

public interface MetricReporterConfigurator extends ModuleConfigurator<MetricReporterConfiguration> {

    @Override
    default ModuleType getType() {
        return ModuleType.METRIC_REPORTER;
    }
}
