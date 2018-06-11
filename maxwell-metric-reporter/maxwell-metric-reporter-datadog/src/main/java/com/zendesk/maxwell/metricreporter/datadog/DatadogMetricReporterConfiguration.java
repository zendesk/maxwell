package com.zendesk.maxwell.metricreporter.datadog;

import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.api.monitoring.MetricReporterConfiguration;
import org.apache.commons.lang3.StringUtils;

public class DatadogMetricReporterConfiguration implements MetricReporterConfiguration {
    public static final String DEFAULT_METRICS_DATADOG_TYPE = "udp";
    public static final String DEFAULT_METRICS_DATADOG_TAGS = "";
    public static final String DEFAULT_METRICS_DATADOG_APIKEY = "";
    public static final String DEFAULT_METRICS_DATADOG_HOST = "localhost";
    public static final int DEFAULT_METRICS_DATADOG_PORT = 8125;
    public static final long DEFAULT_METRICS_DATADOG_INTERVAL = 60L;

    private String type;
    private String tags;
    private String apiKey;
    private String host;
    private int port;
    private Long interval;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Long getInterval() {
        return interval;
    }

    public void setInterval(Long interval) {
        this.interval = interval;
    }

    @Override
    public void validate() {
        if (type != null && type.contains("http") && StringUtils.isEmpty(apiKey)) {
            throw new InvalidOptionException("please specify metrics_datadog_apikey when metrics_datadog_type = http");
        }
    }
}
