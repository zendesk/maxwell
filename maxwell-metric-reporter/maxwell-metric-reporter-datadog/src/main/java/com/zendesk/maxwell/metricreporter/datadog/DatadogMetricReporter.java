package com.zendesk.maxwell.metricreporter.datadog;

import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.metricreporter.core.MetricReporter;
import org.apache.commons.lang3.StringUtils;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.transport.HttpTransport;
import org.coursera.metrics.datadog.transport.Transport;
import org.coursera.metrics.datadog.transport.UdpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.coursera.metrics.datadog.DatadogReporter.Expansion.*;

@Service
public class DatadogMetricReporter implements MetricReporter<DatadogMetricReporterConfiguration> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatadogMetricReporter.class);

    private final MetricRegistry metricRegistry;

    @Autowired
    public DatadogMetricReporter(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    @Override
    public void start(DatadogMetricReporterConfiguration configuration) {
        Transport transport;
        if (configuration.type.contains("http")) {
            LOGGER.info("Enabling HTTP Datadog reporting");
            transport = new HttpTransport.Builder()
                    .withApiKey(configuration.apiKey)
                    .build();
        } else {
            LOGGER.info("Enabling UDP Datadog reporting with host " + configuration.host + ", port " + configuration.port);
            transport = new UdpTransport.Builder()
                    .withStatsdHost(configuration.host)
                    .withPort(configuration.port)
                    .build();
        }

        final DatadogReporter reporter = DatadogReporter.forRegistry(metricRegistry)
                .withTransport(transport)
                .withExpansions(EnumSet.of(COUNT, RATE_1_MINUTE, RATE_15_MINUTE, MEDIAN, P95, P99))
                .withTags(getTags(configuration.tags))
                .build();

        reporter.start(configuration.interval, TimeUnit.SECONDS);
        LOGGER.info("Datadog reporting enabled");
    }

    private static List<String> getTags(String datadogTags) {
        List<String> tags = new ArrayList<>();
        for (String tag : datadogTags.split(",")) {
            if (!StringUtils.isEmpty(tag)) {
                tags.add(tag);
            }
        }

        return tags;
    }
}
