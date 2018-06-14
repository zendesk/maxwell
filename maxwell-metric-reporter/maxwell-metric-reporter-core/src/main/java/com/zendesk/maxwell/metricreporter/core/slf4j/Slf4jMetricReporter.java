package com.zendesk.maxwell.metricreporter.core.slf4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.zendesk.maxwell.metricreporter.core.MetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class Slf4jMetricReporter implements MetricReporter<Slf4jMetricReporterConfiguration> {
    private final static Logger LOGGER = LoggerFactory.getLogger(Slf4jMetricReporter.class);

    private final MetricRegistry metricRegistry;

    @Autowired
    public Slf4jMetricReporter(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public void start(Slf4jMetricReporterConfiguration configuration){
        final Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
                .outputTo(LOGGER)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(configuration.interval, TimeUnit.SECONDS);
        LOGGER.info("Slf4j metrics reporter enabled");
    }
}
