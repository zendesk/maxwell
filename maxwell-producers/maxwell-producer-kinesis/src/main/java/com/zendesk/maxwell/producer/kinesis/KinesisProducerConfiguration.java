package com.zendesk.maxwell.producer.kinesis;

import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;

public class KinesisProducerConfiguration implements ProducerConfiguration {
	public final String kinesisStream;
	public final boolean kinesisMd5Keys;

	public KinesisProducerConfiguration(String kinesisStream, boolean kinesisMd5Keys) {
		this.kinesisStream = kinesisStream;
		this.kinesisMd5Keys = kinesisMd5Keys;
	}

	@Override
	public void validate() {
		if(kinesisStream == null) {
			throw new InvalidOptionException("please specify a stream name for kinesis", "kinesis_stream");
		}
	}
}
