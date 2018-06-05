package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.core.config.MaxwellConfig;

public interface ProducerConfiguration {
	default void mergeWith(MaxwellConfig config){}
	default void validate(){}
}
