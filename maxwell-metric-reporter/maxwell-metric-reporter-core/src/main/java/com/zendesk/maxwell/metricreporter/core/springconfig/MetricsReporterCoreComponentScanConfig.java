package com.zendesk.maxwell.metricreporter.core.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.metricreporter.core")
public class MetricsReporterCoreComponentScanConfig {
}
