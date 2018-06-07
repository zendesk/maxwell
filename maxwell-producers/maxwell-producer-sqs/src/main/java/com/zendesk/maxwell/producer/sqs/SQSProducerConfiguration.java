package com.zendesk.maxwell.producer.sqs;

import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.api.producer.ProducerConfiguration;

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
