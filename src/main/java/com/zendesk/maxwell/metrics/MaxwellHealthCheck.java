package com.zendesk.maxwell.metrics;

import com.codahale.metrics.health.HealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MaxwellHealthCheck extends HealthCheck {

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHealthCheck.class);

	public MaxwellHealthCheck() { }

	@Override
	protected Result check() throws Exception {
		// TODO: actually return something useful
		return Result.healthy();
	}
}