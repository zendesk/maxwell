package com.zendesk.maxwell.producer;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DiagnosticCallbackTest {

	@Test
	public void testCompleteNormally() throws Exception {
		// Given
		KafkaProducerDiagnostic.DiagnosticCallback callback = new KafkaProducerDiagnostic.DiagnosticCallback();

		// When
		callback.onCompletion(null, null);

		// Then
		callback.latency.get();
	}

	@Test
	public void testCompleteExceptionally() {
		// Given
		KafkaProducerDiagnostic.DiagnosticCallback callback = new KafkaProducerDiagnostic.DiagnosticCallback();

		// When
		callback.onCompletion(null, new RuntimeException("blah"));

		// Then
		assertTrue(callback.latency.isCompletedExceptionally());
	}
}
