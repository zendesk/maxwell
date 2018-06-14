package com.zendesk.maxwell.producer.pubsub;

import com.zendesk.maxwell.core.producer.ProducerConfiguration;

public class PubsubProducerConfiguration implements ProducerConfiguration {
	public final String projectId;
	public final String topic;
	public final String ddlTopic;

	public PubsubProducerConfiguration(String projectId, String topic, String ddlTopic) {
		this.projectId = projectId;
		this.topic = topic;
		this.ddlTopic = ddlTopic;
	}

}
