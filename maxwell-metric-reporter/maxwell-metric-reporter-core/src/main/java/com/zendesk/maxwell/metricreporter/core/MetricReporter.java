package com.zendesk.maxwell.metricreporter.core;

public interface MetricReporter<C extends MetricReporterConfiguration> {
    void start(C configuration);
}
