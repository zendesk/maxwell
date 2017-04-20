package com.zendesk.maxwell.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.zendesk.maxwell.producer.AbstractAsyncProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MaxwellHealthCheck extends HealthCheck {

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHealthCheck.class);

	private MetricRegistry metricRegistry;

	public MaxwellHealthCheck(MetricRegistry metricRegistry) {
		this.metricRegistry = metricRegistry;
	}

	@Override
	protected Result check() throws Exception {
		// TODO: this should be configurable.
		if (this.metricRegistry.getMeters().get(AbstractAsyncProducer.failedMessageMeterName).getFifteenMinuteRate() > 0) {
			return Result.unhealthy(">1 messages failed to be sent to Kafka in the past 15minutes");
		} else {
			return Result.healthy();
		}
	}
}