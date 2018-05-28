package com.zendesk.maxwell.core.producer.impl.stdout;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class StdoutProducerFactory implements NamedProducerFactory {
	@Override
	public String getName() {
		return "stdout";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		return new StdoutProducer(context);
	}
}
