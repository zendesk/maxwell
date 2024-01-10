package com.zendesk.maxwell.monitoring;

import com.codahale.metrics.*;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jvm.*;
import com.viafoura.metrics.datadog.DatadogReporter;
import com.viafoura.metrics.datadog.transport.HttpTransport;
import com.viafoura.metrics.datadog.transport.Transport;
import com.viafoura.metrics.datadog.transport.UdpTransport;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.Collections;

import static com.viafoura.metrics.datadog.DatadogReporter.Expansion.*;

public class MaxwellMetrics implements Metrics {

	static final String reportingTypeSlf4j = "slf4j";
	static final String reportingTypeJmx = "jmx";
	static final String reportingTypeHttp = "http";
	static final String reportingTypeDataDog = "datadog";
	static final String reportingTypeStackdriver = "stackdriver";

	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMetrics.class);
	private final ArrayList<Reporter> reporters = new ArrayList<>();
	private final MetricRegistry registry;
	private String metricsPrefix;

	public MaxwellMetrics(MaxwellConfig config, MetricRegistry registry) {
		this.registry = registry;
		setup(config);
	}

	private void setup(MaxwellConfig config) {
		metricsPrefix = config.metricsPrefix;

		if (config.metricsReportingType == null) {
			if ( hasMetricsConfig(config) ) {
				LOGGER.info("Found HTTP server configuration, enabling HTTP-based metrics");
				config.metricsReportingType = "http";
			} else {
				return;
			}
		}

		if (config.metricsJvm) {
			this.registry.register(metricName("jvm", "memory_usage"), new MemoryUsageGaugeSet());
			this.registry.register(metricName("jvm", "gc"), new GarbageCollectorMetricSet());
			this.registry.register(metricName("jvm", "class_loading"), new ClassLoadingGaugeSet());
			this.registry.register(metricName("jvm", "file_descriptor_ratio"), new FileDescriptorRatioGauge());
			this.registry.register(metricName("jvm", "thread_states"), new CachedThreadStatesGaugeSet(60, TimeUnit.SECONDS));
		}

		if (config.metricsReportingType.contains(reportingTypeSlf4j)) {
			final Slf4jReporter reporter = Slf4jReporter.forRegistry(this.registry)
					.outputTo(LOGGER)
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS)
					.build();

			reporter.start(config.metricsSlf4jInterval, TimeUnit.SECONDS);
			reporters.add(reporter);
			LOGGER.info("Slf4j metrics reporter enabled");
		}

		if (config.metricsReportingType.contains(reportingTypeJmx)) {
			final JmxReporter jmxReporter = JmxReporter.forRegistry(this.registry)
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS)
					.build();
			jmxReporter.start();
			reporters.add(jmxReporter);
			LOGGER.info("JMX metrics reporter enabled");

			if (System.getProperty("com.sun.management.jmxremote") == null) {
				LOGGER.warn("JMX remote is disabled");
			} else {
				String portString = System.getProperty("com.sun.management.jmxremote.port");
				if (portString != null) {
					LOGGER.info("JMX running on port " + Integer.parseInt(portString));
				}
			}
		}

		if (config.metricsReportingType.contains(reportingTypeDataDog)) {
			Transport transport;
			if (config.metricsDatadogType.contains("http")) {
				LOGGER.info("Enabling HTTP Datadog reporting to site " + config.metricsDatadogSite);
				HttpTransport.Builder builder = new HttpTransport.Builder()
						.withApiKey(config.metricsDatadogAPIKey);
				if (config.metricsDatadogSite.contains("eu")) {
					builder.withEuSite();
				}
				transport = builder.build();
			} else {
				LOGGER.info("Enabling UDP Datadog reporting with host " + config.metricsDatadogHost
						+ ", port " + config.metricsDatadogPort);
				transport = new UdpTransport.Builder()
						.withStatsdHost(config.metricsDatadogHost)
						.withPort(config.metricsDatadogPort)
						.build();
			}

			final DatadogReporter reporter = DatadogReporter.forRegistry(this.registry)
					.withTransport(transport)
					.withExpansions(EnumSet.of(COUNT, RATE_1_MINUTE, RATE_15_MINUTE, MEDIAN, P95, P99))
					.withTags(getDatadogTags(config.metricsDatadogTags))
					.build();

			reporter.start(config.metricsDatadogInterval, TimeUnit.SECONDS);
			reporters.add(reporter);
			LOGGER.info("Datadog reporting enabled");
		}

		if (config.metricsReportingType.contains(reportingTypeStackdriver)) {
			io.opencensus.metrics.Metrics.getExportComponent().getMetricProducerManager().add(
				new io.opencensus.contrib.dropwizard.DropWizardMetrics(
				  Collections.singletonList(this.registry)));

			try {
				StackdriverStatsExporter.createAndRegister();
			} catch (java.io.IOException e) {
				LOGGER.error("Maxwell encountered an error in creating the stackdriver exporter.", e);
			}

			LOGGER.info("Stackdriver metrics reporter enabled");
		}

		if (config.metricsReportingType.contains(reportingTypeHttp)) {
			CollectorRegistry.defaultRegistry.register(new DropwizardExports(this.registry));
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

	public void stop() throws IOException {
		for ( Reporter r : reporters ) {
			r.close();
		}
	}

	@Override
	public MetricRegistry getRegistry() {
		return this.registry;
	}

	@Override
	public <T extends Metric> void register(String name, T metric) throws IllegalArgumentException {
		getRegistry().register(name, metric);
	}

	static class Registries {
		final MetricRegistry metricRegistry;
		final HealthCheckRegistry healthCheckRegistry;

		Registries(MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry) {
			this.metricRegistry = metricRegistry;
			this.healthCheckRegistry = healthCheckRegistry;
		}
	}

	private boolean hasMetricsConfig(MaxwellConfig config) {
		return config.httpPort != 8080
				|| !config.httpPathPrefix.equals("/")
				|| config.httpBindAddress != null;
	}
}
