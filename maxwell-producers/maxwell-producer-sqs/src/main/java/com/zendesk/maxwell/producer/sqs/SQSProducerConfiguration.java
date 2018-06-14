package com.zendesk.maxwell.producer.sqs;

import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;

public class SQSProducerConfiguration implements ProducerConfiguration {
	public final String queueUri;

	public SQSProducerConfiguration(String queueUri) {
		this.queueUri = queueUri;
	}

	@Override
	public void validate() {
		if(queueUri == null) {
			throw new InvalidOptionException("please specify a queue uri for sqs", "sqs_queue_uri");
		}
	}
}
