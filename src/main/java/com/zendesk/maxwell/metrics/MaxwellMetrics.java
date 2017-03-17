package com.zendesk.maxwell.metrics;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.health.HealthCheckRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MaxwellMetrics {
	public static final MetricRegistry metricRegistry = new MetricRegistry();
	public static final HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

	public static final String metricsName = "MaxwellMetrics";
	public static final String reportingTypeSlf4j = "slf4j";
	public static final String reportingTypeJmx = "jmx";
	public static final String reportingTypeHttp = "http";

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMetrics.class);

	public static void setup(String metricsReportingType, Long metricsReportingInteval, int metricsReportingPort) {
		if (metricsReportingType == null) {
			LOGGER.warn("Metrics will not be exposed: metricsReportingType not configured.");
			return;
		}

		if (metricsReportingType.contains(reportingTypeSlf4j)) {
			final Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
					.outputTo(LOGGER)
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS)
					.build();

			reporter.start(metricsReportingInteval, TimeUnit.SECONDS);
			LOGGER.info("Slf4j metrics reporter enabled");
		}

		if (metricsReportingType.contains(reportingTypeJmx)) {
			final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS)
					.build();
			jmxReporter.start();
			LOGGER.info("Jmx metrics reporter enabled");

			if (System.getProperty("com.sun.management.jmxremote") == null) {
				LOGGER.warn("JMX remote is disabled");
			} else {
				String portString = System.getProperty("com.sun.management.jmxremote.port");
				if (portString != null) {
					LOGGER.info("JMX running on port " + Integer.parseInt(portString));
				}
			}
		}

		if (metricsReportingType.contains(reportingTypeHttp)) {
			healthCheckRegistry.register("MaxwellHealth", new MaxwellHealthCheck(metricRegistry));

			LOGGER.info("Metrics http server starting");
			new MaxwellHTTPServer(metricsReportingPort, MaxwellMetrics.metricRegistry, healthCheckRegistry);
			LOGGER.info("Metrics http server started");
		}
	}
}
