package com.zendesk.maxwell.core.producer.impl.profiler;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class ProfilerProducerFactory implements NamedProducerFactory {
	@Override
	public String getName() {
		return "profiler";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		return new ProfilerProducer(context);
	}
}
