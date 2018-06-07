package com.zendesk.maxwell.api.producer;

import com.zendesk.maxwell.api.config.MaxwellConfig;

public interface ProducerConfiguration {
	default void mergeWith(MaxwellConfig config){}
	default void validate(){}
}