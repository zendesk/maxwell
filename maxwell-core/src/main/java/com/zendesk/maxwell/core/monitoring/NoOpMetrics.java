package com.zendesk.maxwell.core.monitoring;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

public class NoOpMetrics implements Metrics {
	public final MetricRegistry metricRegistry;

	public NoOpMetrics() {
		metricRegistry = new MetricRegistry();
	}

	public String metricName(String... names) {
		return MetricRegistry.name("noop", names);
	}

	public MetricRegistry getRegistry() {
		return metricRegistry;
	}

	@Override
	public <T extends Metric> void register(String name, T metric) throws IllegalArgumentException {
	}

	@Override
	public void unregisterAll() {
	}
}
