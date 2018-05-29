package com.zendesk.maxwell.core.producer.impl.buffered;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class BufferedProducerFactory implements NamedProducerFactory {
	@Override
	public String getName() {
		return "buffer";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		return new BufferedProducer(context, context.getConfig().getBufferedProducerSize());
	}
}
