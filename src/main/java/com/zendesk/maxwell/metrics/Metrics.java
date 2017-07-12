package com.zendesk.maxwell.metrics;

import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.MaxwellContext;

public interface Metrics {
	String metricName(String... names);
	MetricRegistry getRegistry();
}

