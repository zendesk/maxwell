package com.zendesk.maxwell.metricreporter.core.jmx;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.api.monitoring.MetricReporter;
import com.zendesk.maxwell.api.monitoring.MetricReporterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class JmxMetricReporter implements MetricReporter<MetricReporterConfiguration> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxMetricReporter.class);

    private final MetricRegistry metricRegistry;

    @Autowired
    public JmxMetricReporter(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    @Override
    public void start(MetricReporterConfiguration configuration) {
        final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        jmxReporter.start();
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
}
