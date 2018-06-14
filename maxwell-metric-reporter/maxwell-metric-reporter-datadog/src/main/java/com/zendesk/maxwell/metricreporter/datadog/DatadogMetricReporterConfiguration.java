package com.zendesk.maxwell.metricreporter.datadog;

import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.metricreporter.core.MetricReporterConfiguration;
import org.apache.commons.lang3.StringUtils;

public class DatadogMetricReporterConfiguration implements MetricReporterConfiguration {
    public static final String DEFAULT_METRICS_DATADOG_TYPE = "udp";
    public static final String DEFAULT_METRICS_DATADOG_TAGS = "";
    public static final String DEFAULT_METRICS_DATADOG_APIKEY = "";
    public static final String DEFAULT_METRICS_DATADOG_HOST = "localhost";
    public static final int DEFAULT_METRICS_DATADOG_PORT = 8125;
    public static final long DEFAULT_METRICS_DATADOG_INTERVAL = 60L;

    public String type;
    public String tags;
    public String apiKey;
    public String host;
    public int port;
    public Long interval;

    @Override
    public void validate() {
        if (type != null && type.contains("http") && StringUtils.isEmpty(apiKey)) {
            throw new InvalidOptionException("please specify metrics_datadog_apikey when metrics_datadog_type = http");
        }
    }
}
