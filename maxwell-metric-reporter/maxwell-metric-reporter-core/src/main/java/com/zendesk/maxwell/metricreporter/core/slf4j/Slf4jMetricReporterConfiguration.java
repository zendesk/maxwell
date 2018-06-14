package com.zendesk.maxwell.metricreporter.core.slf4j;

import com.zendesk.maxwell.metricreporter.core.MetricReporterConfiguration;

public class Slf4jMetricReporterConfiguration implements MetricReporterConfiguration {
    public static final long DEFAULT_SLF4J_INTERVAL = 60L;

    public final long interval;

    public Slf4jMetricReporterConfiguration(long interval) {
        this.interval = interval;
    }

}
