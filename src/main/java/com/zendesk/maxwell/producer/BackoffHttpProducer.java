package com.zendesk.maxwell.producer;

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.ExponentialBackOff;
import com.zendesk.maxwell.MaxwellContext;
import sun.nio.ch.Net;

import java.io.IOException;

/**
 * a simple HTTP POST producer with exponential backoff.
 */
public class BackoffHttpProducer extends AbstractHttpProducer {

	final static int MAX_RETRY_SECONDS = 15;

	public BackoffHttpProducer(MaxwellContext context, String endpoint) {
		super(context, endpoint);
	}

	public BackoffHttpProducer(MaxwellContext context, HttpTransport transport, String endpoint) {
		super(context, transport, endpoint);
	}

	@Override
	public void initialize(HttpRequest request) throws IOException {
		ExponentialBackOff backoff = new ExponentialBackOff.Builder()
				.setInitialIntervalMillis(500)
				.setMaxElapsedTimeMillis(1000 * MAX_RETRY_SECONDS)
				.setMaxIntervalMillis(6000)
				.setMultiplier(1.5)
				.setRandomizationFactor(0.5)
				.build();

		request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backoff));
	}

	//@Override
	public void signRequest(HttpRequest request, String payload) throws Exception {
		// default do nothing
	}
}
