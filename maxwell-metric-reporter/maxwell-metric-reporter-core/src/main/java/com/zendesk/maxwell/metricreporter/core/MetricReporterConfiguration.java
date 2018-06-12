package com.zendesk.maxwell.metricreporter.core;

public interface MetricReporterConfiguration {
    MetricReporterConfiguration EMTPY = new MetricReporterConfiguration() { };

    default void validate(){}
}
