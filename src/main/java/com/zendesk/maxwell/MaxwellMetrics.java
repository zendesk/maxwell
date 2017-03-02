package com.zendesk.maxwell;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MaxwellMetrics {
	public static final MetricRegistry registry = new MetricRegistry();

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMetrics.class);

	public static void setup(String metricsReportingType, Long metricsReportingInteval) {
		// TODO: csvreporter? consolereporter? (console feels kinda like a dup of slf4j)
		if (metricsReportingType.equalsIgnoreCase("slf4j")) {
			final Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
					.outputTo(LoggerFactory.getLogger("com.zendesk.maxwell.metrics"))
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS)
					.withLoggingLevel(Slf4jReporter.LoggingLevel.DEBUG)
					.build();
			reporter.start(metricsReportingInteval, TimeUnit.SECONDS);
			LOGGER.info("Slf4j metrics reporter enabled");
		} else if (metricsReportingType.equalsIgnoreCase("jmx")) {
			final JmxReporter jmxReporter = JmxReporter.forRegistry(registry)
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS)
					.build();
			jmxReporter.start();
			LOGGER.info("Jmx metrics reporter enabled");
		} else {
			LOGGER.warn("Metrics reporter not enabled");
		}
	}
}
