package com.zendesk.maxwell.core.producer.impl.profiler;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;
import com.zendesk.maxwell.core.producer.ProducerConfigurator;
import org.springframework.stereotype.Service;

@Service
public class ProfilerProducerConfigurator implements ProducerConfigurator {

	@Override
	public String getIdentifier() {
		return "profiler";
	}

	@Override
	public Producer configure(MaxwellContext maxwellContext, ProducerConfiguration configuration) {
		return new ProfilerProducer(maxwellContext);
	}
}
