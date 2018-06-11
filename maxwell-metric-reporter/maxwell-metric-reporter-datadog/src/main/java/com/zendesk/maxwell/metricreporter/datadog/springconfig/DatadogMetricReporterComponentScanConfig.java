package com.zendesk.maxwell.metricreporter.datadog.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.metricsreporter.datadog")
public class DatadogMetricReporterComponentScanConfig {
}
