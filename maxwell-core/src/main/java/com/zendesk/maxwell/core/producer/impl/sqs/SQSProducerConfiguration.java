package com.zendesk.maxwell.core.producer.impl.sqs;

import com.zendesk.maxwell.core.config.ExtensionConfiguration;
import com.zendesk.maxwell.core.config.InvalidOptionException;
import com.zendesk.maxwell.core.config.MaxwellConfig;

public class SQSProducerConfiguration implements ExtensionConfiguration {
	private final String sqsQueueUri;

	public SQSProducerConfiguration(String sqsQueueUri) {
		this.sqsQueueUri = sqsQueueUri;
	}

	public String getSqsQueueUri() {
		return sqsQueueUri;
	}

	@Override
	public void validate(MaxwellConfig maxwellConfig) {
		if(sqsQueueUri == null) {
			throw new InvalidOptionException("please specify a queue uri for sqs", "sqs_queue_uri");
		}
	}
}
