package com.zendesk.maxwell.metricreporter.http.springconfig;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsTestConfig {

	@Bean
	public MetricRegistry metricRegistry(){
		return new MetricRegistry();
	}

	@Bean
	public HealthCheckRegistry healthCheckRegistry(){
		return new HealthCheckRegistry();
	}

}
