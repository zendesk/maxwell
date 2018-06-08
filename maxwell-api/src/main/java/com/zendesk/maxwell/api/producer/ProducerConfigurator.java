package com.zendesk.maxwell.api.producer;

import com.zendesk.maxwell.api.config.ModuleConfigurator;
import com.zendesk.maxwell.api.config.ModuleType;

public interface ProducerConfigurator extends ModuleConfigurator<ProducerConfiguration> {

	@Override
	default ModuleType getType() {
		return ModuleType.PRODUCER;
	}

	Class<? extends ProducerFactory> getFactory();

}
