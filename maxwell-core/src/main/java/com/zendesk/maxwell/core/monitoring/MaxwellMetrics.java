package com.zendesk.maxwell.core.monitoring;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.*;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MaxwellMetrics implements Metrics {

	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMetrics.class);
	private final MetricRegistry metricRegistry;
	private String metricsPrefix;
	private final List<String> registeredMetrics = new ArrayList<>();

	public MaxwellMetrics(MaxwellConfig config, MetricRegistry metricRegistry) {
		this.metricRegistry = metricRegistry;
		setup(config);
	}

	private void setup(MaxwellConfig config) {
		if (config.getMetricsReportingType() == null) {
			LOGGER.warn("Metrics will not be exposed: metricsReportingType not configured.");
			return;
		}

		metricsPrefix = config.getMetricsPrefix();

		if (config.isMetricsJvm()) {
			metricRegistry.register(metricName("jvm", "memory_usage"), new MemoryUsageGaugeSet());
			metricRegistry.register(metricName("jvm", "gc"), new GarbageCollectorMetricSet());
			metricRegistry.register(metricName("jvm", "class_loading"), new ClassLoadingGaugeSet());
			metricRegistry.register(metricName("jvm", "file_descriptor_ratio"), new FileDescriptorRatioGauge());
			metricRegistry.register(metricName("jvm", "thread_states"), new CachedThreadStatesGaugeSet(60, TimeUnit.SECONDS));
		}
	}

	public String metricName(String... names) {
		return MetricRegistry.name(metricsPrefix, names);
	}

	@Override
	public MetricRegistry getRegistry() {
		return metricRegistry;
	}

	@Override
	public <T extends Metric> void register(String name, T metric) throws IllegalArgumentException {
		registeredMetrics.add(name);
		getRegistry().register(name, metric);
	}

	@Override
	public void unregisterAll(){
		for(String name : registeredMetrics){
			getRegistry().remove(name);
		}
	}
}
