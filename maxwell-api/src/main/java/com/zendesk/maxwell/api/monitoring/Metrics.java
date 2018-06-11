package com.zendesk.maxwell.api.monitoring;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

public interface Metrics {

	String REPORTING_TYPE_SLF4J = "slf4j";
	String REPORTING_TYPE_JMX = "jmx";
	String REPORTING_TYPE_HTTP = "http";
	String REPORTING_TYPE_DATADOG = "datadog";

	String metricName(String... names);
	MetricRegistry getRegistry();
	<T extends Metric> void register(String name, T metric) throws IllegalArgumentException;
	void unregisterAll();
}

