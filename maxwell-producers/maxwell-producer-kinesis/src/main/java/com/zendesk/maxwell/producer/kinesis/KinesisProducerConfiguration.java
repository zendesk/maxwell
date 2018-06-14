package com.zendesk.maxwell.producer.kinesis;

import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;

public class KinesisProducerConfiguration implements ProducerConfiguration {
	public final String stream;
	public final boolean md5Keys;

	public KinesisProducerConfiguration(String stream, boolean md5Keys) {
		this.stream = stream;
		this.md5Keys = md5Keys;
	}

	@Override
	public void validate() {
		if(stream == null) {
			throw new InvalidOptionException("please specify a stream name for kinesis", "kinesis_stream");
		}
	}
}
