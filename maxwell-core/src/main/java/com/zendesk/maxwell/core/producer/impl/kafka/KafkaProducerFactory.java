package com.zendesk.maxwell.core.producer.impl.kafka;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerFactory implements NamedProducerFactory  {
	@Override
	public String getName() {
		return "kafka";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		return new MaxwellKafkaProducer(context, context.getConfig().getKafkaProperties(), context.getConfig().getKafkaTopic());
	}
}
