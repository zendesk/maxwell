package com.zendesk.maxwell.example.maxwellhealthcheckfactory;

import com.zendesk.maxwell.monitoring.MaxwellHealthCheck;
import com.zendesk.maxwell.producer.AbstractProducer;

public class CustomMaxwellHealthCheck extends MaxwellHealthCheck {
	public CustomMaxwellHealthCheck(AbstractProducer producer) {
		super(producer);
	}

	@Override
	protected Result check() throws Exception {
		return Result.unhealthy("I am always unhealthy");
	}
}
