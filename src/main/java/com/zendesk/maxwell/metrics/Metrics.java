package com.zendesk.maxwell.metrics;

import com.codahale.metrics.MetricRegistry;

public interface Metrics {
	String metricName(String... names);
	MetricRegistry getRegistry();
}

