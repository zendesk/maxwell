package com.zendesk.maxwell.standalone.springconfig;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.standalone.spring.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.standalone")
@EnableAutoConfiguration
public class StandaloneApplicationComponentScanConfig {

	@Bean
	public MetricRegistry metricRegistry(){
		return new MetricRegistry();
	}

	@Bean
	public HealthCheckRegistry healthCheckRegistry(){
		return new HealthCheckRegistry();
	}
}
