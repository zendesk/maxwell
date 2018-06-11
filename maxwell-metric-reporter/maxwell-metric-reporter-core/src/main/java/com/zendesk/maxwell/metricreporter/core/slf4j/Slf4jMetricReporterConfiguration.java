package com.zendesk.maxwell.metricreporter.core.slf4j;

import com.zendesk.maxwell.api.monitoring.MetricReporterConfiguration;

public class Slf4jMetricReporterConfiguration implements MetricReporterConfiguration {
    public static final long DEFAULT_METRITCS_SLF4J_INTERVAL = 60L;

    private final long interval;

    public Slf4jMetricReporterConfiguration(long interval) {
        this.interval = interval;
    }

    public long getInterval() {
        return interval;
    }
}
