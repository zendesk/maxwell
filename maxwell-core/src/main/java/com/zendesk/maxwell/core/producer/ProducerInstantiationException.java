package com.zendesk.maxwell.core.producer;

public class ProducerInstantiationException extends RuntimeException {
	public ProducerInstantiationException(String message, Throwable cause) {
		super(message, cause);
	}

	public ProducerInstantiationException(Throwable cause) {
		super(cause);
	}
}
