package com.zendesk.maxwell.api.monitoring;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

public interface Metrics {
	String metricName(String... names);
	MetricRegistry getRegistry();
	<T extends Metric> void register(String name, T metric) throws IllegalArgumentException;
	void unregisterAll();
}

