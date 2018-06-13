package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.health.HealthCheck;

public class ProducerHealthCheck extends HealthCheck {
	private final Meter failedMessageMeter;

	public ProducerHealthCheck(Producer producer) {
		this.failedMessageMeter = producer.getFailedMessageMeter();
	}

	@Override
	protected Result check() throws Exception {
		// TODO: this should be configurable.
		if (failedMessageMeter != null && failedMessageMeter.getFifteenMinuteRate() > 0) {
			return Result.unhealthy(">1 messages failed to be sent to Kafka in the past 15minutes");
		} else {
			return Result.healthy();
		}
	}
}
