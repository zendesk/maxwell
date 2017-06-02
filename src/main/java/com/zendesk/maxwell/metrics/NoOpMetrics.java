package com.zendesk.maxwell.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;

public class NoOpMetrics implements Metrics {
	public final MetricRegistry metricRegistry;
	public final HealthCheckRegistry healthCheckRegistry;

	public NoOpMetrics() {
		metricRegistry = new MetricRegistry();
		healthCheckRegistry = new HealthCheckRegistry();
	}

	public String metricName(String... names) {
		return MetricRegistry.name("noop", names);
	}

	public MetricRegistry getRegistry() {
		return metricRegistry;
	}
}
