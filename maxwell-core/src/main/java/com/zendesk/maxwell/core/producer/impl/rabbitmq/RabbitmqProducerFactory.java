package com.zendesk.maxwell.core.producer.impl.rabbitmq;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class RabbitmqProducerFactory implements NamedProducerFactory {
	@Override
	public String getName() {
		return "rabbitmq";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		return new RabbitmqProducer(context);
	}
}
