package com.zendesk.maxwell.core.producer.impl.sqs;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class SQSProducerFactory implements NamedProducerFactory {
	@Override
	public String getName() {
		return "sqs";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		return new MaxwellSQSProducer(context, context.getConfig().getSqsQueueUri());
	}
}
