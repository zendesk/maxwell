package com.zendesk.maxwell.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.MaxwellContext;

public interface Metrics {
	String metricName(String... names);
	MetricRegistry getRegistry();
	<T extends Metric> void register(String name, T metric) throws IllegalArgumentException;
}

