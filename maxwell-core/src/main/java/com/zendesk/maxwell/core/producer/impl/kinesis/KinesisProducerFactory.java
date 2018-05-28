package com.zendesk.maxwell.core.producer.impl.kinesis;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class KinesisProducerFactory implements NamedProducerFactory {
	@Override
	public String getName() {
		return "kinesis";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		return new MaxwellKinesisProducer(context, context.getConfig().kinesisStream);
	}
}
