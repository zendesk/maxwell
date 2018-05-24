package com.zendesk.maxwell.replication;

import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HeartbeatObserverTest {

	@Test
	public void testObserverIsAddedToNotifier() {
		// Given
		HeartbeatNotifier notifier = new HeartbeatNotifier();
		new BinlogConnectorDiagnostic.HeartbeatObserver(notifier, Clock.systemUTC());

		// When

		// Then
		assertThat(notifier.countObservers(), is(1));
	}

	@Test
	public void testObserverIsRemovedAfterUpdate() {
		// Given
		HeartbeatNotifier notifier = new HeartbeatNotifier();
		new BinlogConnectorDiagnostic.HeartbeatObserver(notifier, Clock.systemUTC());

		// When
		notifier.heartbeat(12345);

		// Then
		assertThat(notifier.countObservers(), is(0));
	}

	@Test
	public void testLatencyFutureShouldComplete() throws ExecutionException, InterruptedException {
		// Given
		HeartbeatNotifier notifier = new HeartbeatNotifier();
		Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
		BinlogConnectorDiagnostic.HeartbeatObserver observer = new BinlogConnectorDiagnostic.HeartbeatObserver(notifier, clock);

		long now = clock.millis();
		long heartbeat = now - 10;

		// When
		notifier.heartbeat(heartbeat);

		// Then
		assertThat(observer.latency.get(), is(10L));
	}

	@Test
	public void testFail() {
		// Given
		BinlogConnectorDiagnostic.HeartbeatObserver observer = new BinlogConnectorDiagnostic.HeartbeatObserver(new HeartbeatNotifier(), Clock.systemUTC());

		// When
		RuntimeException ex = new RuntimeException("blah");
		observer.fail(ex);

		// Then
		assertTrue(observer.latency.isCompletedExceptionally());
		try {
			observer.latency.get();
			fail();
		} catch (InterruptedException | ExecutionException e) {
			assertThat(e.getCause(), is(ex));
		}
	}
}
