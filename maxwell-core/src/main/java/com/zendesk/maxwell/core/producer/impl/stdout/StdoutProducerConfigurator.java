package com.zendesk.maxwell.core.producer.impl.stdout;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;
import com.zendesk.maxwell.core.producer.ProducerConfigurator;
import org.springframework.stereotype.Service;

@Service
public class StdoutProducerConfigurator implements ProducerConfigurator {
	@Override
	public String getIdentifier() {
		return "stdout";
	}

	@Override
	public Producer configure(MaxwellContext maxwellContext, ProducerConfiguration configuration) {
		return new StdoutProducer(maxwellContext);
	}
}
