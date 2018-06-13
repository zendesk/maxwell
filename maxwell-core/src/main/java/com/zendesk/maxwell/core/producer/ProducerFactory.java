package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;

public interface ProducerFactory {
	Producer createProducer(MaxwellContext context);
}
