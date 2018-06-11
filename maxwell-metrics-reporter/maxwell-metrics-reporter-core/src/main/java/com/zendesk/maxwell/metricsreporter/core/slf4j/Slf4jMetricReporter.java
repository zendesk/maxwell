package com.zendesk.maxwell.metricsreporter.core.slf4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.zendesk.maxwell.api.monitoring.MetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;

public class Slf4jMetricReporter implements MetricReporter {
    private final static Logger LOGGER = LoggerFactory.getLogger(Slf4jMetricReporter.class);

    private final Slf4jMetricReporterConfiguration configuration;

    public Slf4jMetricReporter(Slf4jMetricReporterConfiguration configuration) {
        this.configuration = configuration;
    }

    @Autowired
    public void start(MetricRegistry metricRegistry){
        final Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
                .outputTo(LOGGER)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(configuration.getInterval(), TimeUnit.SECONDS);
        LOGGER.info("Slf4j metrics reporter enabled");
    }
}
