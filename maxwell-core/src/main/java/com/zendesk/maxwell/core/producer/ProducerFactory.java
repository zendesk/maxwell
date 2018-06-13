package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.core.MaxwellContext;

public interface ProducerFactory {
	Producer createProducer(MaxwellContext context);
}
