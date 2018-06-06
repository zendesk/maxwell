package com.zendesk.maxwell.core.producer.impl.pubsub;

import com.zendesk.maxwell.api.producer.ProducerConfiguration;

public class PubsubProducerConfiguration implements ProducerConfiguration {
	private String pubsubProjectId;
	private String pubsubTopic;
	private String ddlPubsubTopic;

	public String getPubsubProjectId() {
		return pubsubProjectId;
	}

	public void setPubsubProjectId(String pubsubProjectId) {
		this.pubsubProjectId = pubsubProjectId;
	}

	public String getPubsubTopic() {
		return pubsubTopic;
	}

	public void setPubsubTopic(String pubsubTopic) {
		this.pubsubTopic = pubsubTopic;
	}

	public String getDdlPubsubTopic() {
		return ddlPubsubTopic;
	}

	public void setDdlPubsubTopic(String ddlPubsubTopic) {
		this.ddlPubsubTopic = ddlPubsubTopic;
	}
}
