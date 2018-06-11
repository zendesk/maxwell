package com.zendesk.maxwell.metricreporter.http;

import com.zendesk.maxwell.api.monitoring.MetricReporterConfiguration;

public class HttpMetricReporterConfiguration implements MetricReporterConfiguration {

    public static final int DEFAULT_HTTP_PORT = 8080;
    public static final String DEFAULT_HTTP_PATH_PREFIX = "/";
    public static final boolean DEFAULT_DIAGNOSTIC_HTTP = false;
    public static final long DEFAULT_DIAGNOSTIC_HTTP_TIMEOUT = 10000L;

    private int httpPort;
    private String httpBindAddress;
    private String httpPathPrefix;

    private boolean diagnoticEnabled;
    private long diagnoticTimeout;

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public String getHttpBindAddress() {
        return httpBindAddress;
    }

    public void setHttpBindAddress(String httpBindAddress) {
        this.httpBindAddress = httpBindAddress;
    }

    public String getHttpPathPrefix() {
        return httpPathPrefix;
    }

    public void setHttpPathPrefix(String httpPathPrefix) {
        this.httpPathPrefix = httpPathPrefix;
    }

    public boolean isDiagnoticEnabled() {
        return diagnoticEnabled;
    }

    public void setDiagnoticEnabled(boolean diagnoticEnabled) {
        this.diagnoticEnabled = diagnoticEnabled;
    }

    public long getDiagnoticTimeout() {
        return diagnoticTimeout;
    }

    public void setDiagnoticTimeout(long diagnoticTimeout) {
        this.diagnoticTimeout = diagnoticTimeout;
    }
}
