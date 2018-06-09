package com.zendesk.maxwell.api.monitoring;

import com.zendesk.maxwell.api.config.ModuleConfiguration;

public interface MetricReporterConfiguration extends ModuleConfiguration {
    MetricReporterConfiguration EMTPY = new MetricReporterConfiguration() { };
}
