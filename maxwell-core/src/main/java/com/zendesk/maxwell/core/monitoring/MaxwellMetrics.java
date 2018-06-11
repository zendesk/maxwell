package com.zendesk.maxwell.core.monitoring;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.*;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.monitoring.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.transport.HttpTransport;
import org.coursera.metrics.datadog.transport.Transport;
import org.coursera.metrics.datadog.transport.UdpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.coursera.metrics.datadog.DatadogReporter.Expansion.*;

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

		if (config.getMetricsReportingType().contains(REPORTING_TYPE_DATADOG)) {
			Transport transport;
			if (config.getMetricsDatadogType().contains("http")) {
				LOGGER.info("Enabling HTTP Datadog reporting");
				transport = new HttpTransport.Builder()
						.withApiKey(config.getMetricsDatadogAPIKey())
						.build();
			} else {
				LOGGER.info("Enabling UDP Datadog reporting with host " + config.getMetricsDatadogHost()
						+ ", port " + config.getMetricsDatadogPort());
				transport = new UdpTransport.Builder()
						.withStatsdHost(config.getMetricsDatadogHost())
						.withPort(config.getMetricsDatadogPort())
						.build();
			}

			final DatadogReporter reporter = DatadogReporter.forRegistry(metricRegistry)
					.withTransport(transport)
					.withExpansions(EnumSet.of(COUNT, RATE_1_MINUTE, RATE_15_MINUTE, MEDIAN, P95, P99))
					.withTags(getDatadogTags(config.getMetricsDatadogTags()))
					.build();

			reporter.start(config.getMetricsDatadogInterval(), TimeUnit.SECONDS);
			LOGGER.info("Datadog reporting enabled");
		}
	}

	private static ArrayList<String> getDatadogTags(String datadogTags) {
		ArrayList<String> tags = new ArrayList<>();
		for (String tag : datadogTags.split(",")) {
			if (!StringUtils.isEmpty(tag)) {
				tags.add(tag);
			}
		}

		return tags;
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
