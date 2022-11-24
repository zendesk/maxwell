package com.zendesk.maxwell.replication;

import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import binlogdata.Binlogdata.VEvent;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.vitess.proto.Vtgate;

///
// Observes the VStream response stream, extracts events from VStream responses,
// then passes them to the VStreamReplicator via a queue for processing.
//
public class VStreamObserver implements StreamObserver<Vtgate.VStreamResponse> {
	private static final Logger LOGGER = LoggerFactory.getLogger(VStreamObserver.class);
	private final AtomicBoolean mustStop = new AtomicBoolean(false);
	private final LinkedBlockingDeque<VEvent> queue;
	private Exception lastException = null;

	public VStreamObserver(LinkedBlockingDeque<VEvent> queue) {
		this.queue = queue;
	}

	// Shuts down the observer
	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onNext(Vtgate.VStreamResponse response) {
		LOGGER.debug("Received {} VEvents in the VStreamResponse:", response.getEventsCount());

		List<VEvent> messageEvents = response.getEventsList();
		for (VEvent event : messageEvents) {
			LOGGER.debug("VEvent: {}", event);
			enqueueEvent(event);
		}
	}

	@Override
	public void onError(Throwable t) {
		this.lastException = Status.fromThrowable(t).asException();
		LOGGER.error("VStream streaming onError. Status: {}", lastException);
		stop();
	}

	@Override
	public void onCompleted() {
		LOGGER.info("VStream streaming completed.");
		stop();
	}

	public Exception getLastException() {
		return lastException;
	}

	// Pushes an event to the queue for VStreamReplicator to process.
	private void enqueueEvent(VEvent event) {
		while (mustStop.get() != true) {
			try {
				if (queue.offer(event, 100, TimeUnit.MILLISECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				return;
			}
		}
	}
}
