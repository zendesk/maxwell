package com.zendesk.maxwell.metricreporter.http.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.metricreporter.http")
public class HttpMetricReporterComponentScanConfig {
}
