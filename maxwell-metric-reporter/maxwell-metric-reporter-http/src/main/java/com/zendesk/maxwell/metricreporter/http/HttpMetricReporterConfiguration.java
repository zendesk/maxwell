package com.zendesk.maxwell.metricreporter.http;

import com.zendesk.maxwell.metricreporter.core.MetricReporterConfiguration;

public class HttpMetricReporterConfiguration implements MetricReporterConfiguration {

    public static final int DEFAULT_HTTP_PORT = 8080;
    public static final String DEFAULT_HTTP_PATH_PREFIX = "/";
    public static final boolean DEFAULT_DIAGNOSTIC_HTTP = false;
    public static final long DEFAULT_DIAGNOSTIC_HTTP_TIMEOUT = 10000L;

    public int httpPort;
    public String httpBindAddress;
    public String httpPathPrefix;

    public boolean diagnosticEnabled;
    public long diagnosticTimeout;
}
