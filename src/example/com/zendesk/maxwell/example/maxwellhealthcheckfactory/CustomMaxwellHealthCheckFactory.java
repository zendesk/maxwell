package com.zendesk.maxwell.example.maxwellhealthcheckfactory;

import com.zendesk.maxwell.monitoring.MaxwellHealthCheck;
import com.zendesk.maxwell.monitoring.MaxwellHealthCheckFactory;
import com.zendesk.maxwell.producer.AbstractProducer;

public class CustomMaxwellHealthCheckFactory implements MaxwellHealthCheckFactory
{
	@Override
	public MaxwellHealthCheck createHealthCheck(AbstractProducer producer)
	{
		return new CustomMaxwellHealthCheck(producer);
	}
}
