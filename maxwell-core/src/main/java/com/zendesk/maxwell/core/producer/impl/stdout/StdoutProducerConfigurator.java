package com.zendesk.maxwell.core.producer.impl.stdout;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.ExtensionConfigurator;
import com.zendesk.maxwell.core.config.ExtensionType;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class StdoutProducerConfigurator implements ExtensionConfigurator<Producer> {
	@Override
	public String getExtensionIdentifier() {
		return "stdout";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PRODUCER;
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new StdoutProducer(context);
	}
}
