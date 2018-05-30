package com.zendesk.maxwell.core.producer.impl.buffered;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.ExtensionConfigurator;
import com.zendesk.maxwell.core.config.ExtensionType;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class BufferedExtensionConfigurator implements ExtensionConfigurator<Producer> {
	@Override
	public String getExtensionIdentifier() {
		return "buffer";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PROVIDER;
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new BufferedProducer(context, context.getConfig().getBufferedProducerSize());
	}
}
