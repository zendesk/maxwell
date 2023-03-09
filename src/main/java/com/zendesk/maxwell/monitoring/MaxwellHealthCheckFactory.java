package com.zendesk.maxwell.monitoring;

import com.zendesk.maxwell.monitoring.MaxwellHealthCheck;
import com.zendesk.maxwell.producer.AbstractProducer;

public interface MaxwellHealthCheckFactory {
	MaxwellHealthCheck createHealthCheck(AbstractProducer producer);
}
