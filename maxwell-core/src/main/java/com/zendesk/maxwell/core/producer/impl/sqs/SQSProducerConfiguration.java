package com.zendesk.maxwell.core.producer.impl.sqs;

import com.zendesk.maxwell.core.producer.ProducerConfiguration;
import com.zendesk.maxwell.core.config.InvalidOptionException;

public class SQSProducerConfiguration implements ProducerConfiguration {
	private final String sqsQueueUri;

	public SQSProducerConfiguration(String sqsQueueUri) {
		this.sqsQueueUri = sqsQueueUri;
	}

	public String getSqsQueueUri() {
		return sqsQueueUri;
	}

	@Override
	public void validate() {
		if(sqsQueueUri == null) {
			throw new InvalidOptionException("please specify a queue uri for sqs", "sqs_queue_uri");
		}
	}
}
