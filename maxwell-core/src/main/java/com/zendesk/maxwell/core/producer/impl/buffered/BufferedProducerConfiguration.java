package com.zendesk.maxwell.core.producer.impl.buffered;

import com.zendesk.maxwell.core.producer.ProducerConfiguration;

public class BufferedProducerConfiguration implements ProducerConfiguration {
	public static final int DEFAULT_BUFFER_SIZE = 200;
	private final int bufferedProducerSize;

	public BufferedProducerConfiguration() {
		this(DEFAULT_BUFFER_SIZE);
	}

	public BufferedProducerConfiguration(int bufferedProducerSize) {
		this.bufferedProducerSize = bufferedProducerSize;
	}

	public int getBufferedProducerSize() {
		return bufferedProducerSize;
	}
}
