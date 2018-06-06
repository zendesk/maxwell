package com.zendesk.maxwell.api.producer;

import com.zendesk.maxwell.api.MaxwellContext;

public interface ProducerFactory {
	Producer createProducer(MaxwellContext context);
}
