package com.zendesk.maxwell.producer;

import io.nats.client.Options;
import org.mockito.ArgumentMatcher;

public class NatsOptionMatcher implements ArgumentMatcher<Options> {

	private final Options expected;

	public NatsOptionMatcher(final Options expected) {
		this.expected = expected;
	}

	@Override
	public boolean matches(Options options) {
		return this.expected.getServers().equals(options.getServers());
	}
}
