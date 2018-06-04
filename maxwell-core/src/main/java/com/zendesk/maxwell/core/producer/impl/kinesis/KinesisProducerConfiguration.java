package com.zendesk.maxwell.core.producer.impl.kinesis;

import com.zendesk.maxwell.core.config.ExtensionConfiguration;
import com.zendesk.maxwell.core.config.InvalidOptionException;
import com.zendesk.maxwell.core.config.MaxwellConfig;

public class KinesisProducerConfiguration implements ExtensionConfiguration {
	private final String kinesisStream;
	private final boolean kinesisMd5Keys;

	public KinesisProducerConfiguration(String kinesisStream, boolean kinesisMd5Keys) {
		this.kinesisStream = kinesisStream;
		this.kinesisMd5Keys = kinesisMd5Keys;
	}

	public String getKinesisStream() {
		return kinesisStream;
	}

	public boolean isKinesisMd5Keys() {
		return kinesisMd5Keys;
	}

	@Override
	public void validate(MaxwellConfig maxwellConfig) {
		if(kinesisStream == null) {
			throw new InvalidOptionException("please specify a stream name for kinesis", "kinesis_stream");
		}
	}
}
