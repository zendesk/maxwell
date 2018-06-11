package com.zendesk.maxwell.api.monitoring;

public interface MetricReporter<C extends MetricReporterConfiguration> {
    void start(C configuration);
}
