package com.zendesk.maxwell.api.monitoring;

import com.codahale.metrics.MetricRegistry;

public interface MetricReporter {
    void start(MetricRegistry metricRegistry);
}
