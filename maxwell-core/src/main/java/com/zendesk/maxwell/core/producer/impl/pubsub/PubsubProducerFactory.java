package com.zendesk.maxwell.core.producer.impl.pubsub;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerInstantiationException;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class PubsubProducerFactory implements NamedProducerFactory {
	@Override
	public String getName() {
		return "pubsub";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		try {
			return new MaxwellPubsubProducer(context, context.getConfig().getPubsubProjectId(), context.getConfig().getPubsubTopic(), context.getConfig().getDdlPubsubTopic());
		} catch (IOException e) {
			throw new ProducerInstantiationException(e);
		}
	}
}
