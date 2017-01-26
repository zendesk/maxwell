package com.zendesk.maxwell;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MaxwellMetrics {
	public static final MetricRegistry registry = new MetricRegistry();

	public static void setup() {
		final Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
			.outputTo(LoggerFactory.getLogger("com.zendesk.maxwell.metrics"))
			.convertRatesTo(TimeUnit.SECONDS)
			.convertDurationsTo(TimeUnit.MILLISECONDS)
			.withLoggingLevel(Slf4jReporter.LoggingLevel.DEBUG)
			.build();
		reporter.start(1, TimeUnit.MINUTES);
	}
}
