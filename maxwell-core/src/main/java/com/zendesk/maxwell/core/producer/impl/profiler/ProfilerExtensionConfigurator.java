package com.zendesk.maxwell.core.producer.impl.profiler;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.ExtensionConfigurator;
import com.zendesk.maxwell.core.config.ExtensionType;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class ProfilerExtensionConfigurator implements ExtensionConfigurator<Producer> {

	@Override
	public String getExtensionIdentifier() {
		return "profiler";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PROVIDER;
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return  new ProfilerProducer(context);
	}

}
