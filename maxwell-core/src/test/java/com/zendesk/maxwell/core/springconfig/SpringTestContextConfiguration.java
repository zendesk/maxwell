package com.zendesk.maxwell.core.springconfig;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({CoreComponentScanConfig.class})
public class SpringTestContextConfiguration {

	@Bean
	public MetricRegistry metricRegistry(){
		return new MetricRegistry();
	}

	@Bean
	public HealthCheckRegistry healthCheckRegistry(){
		return new HealthCheckRegistry();
	}

}
