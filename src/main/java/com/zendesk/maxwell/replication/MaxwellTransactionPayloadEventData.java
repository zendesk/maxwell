package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * A {@link TransactionPayloadEventData} that holds the decompressed transaction and parses the
 * inner events on demand instead of materializing them all up front.
 * <p>
 * A compressed transaction is bounded by the transaction size, not by any event-size limit, so
 * parsing every inner event into a list before the first one is consumed puts the whole
 * transaction's object graph on the heap at once — defeating the backpressure of the replicator's
 * bounded event queue. {@link BinlogConnectorEventListener} pulls inner events one at a time via
 * {@link #nextInnerEvent()} while it feeds that queue, so only one un-queued inner event (plus the
 * raw decompressed bytes) is held here at any moment. Once the cursor reaches the end, the buffer
 * and parser are released: the binlog client keeps a reference to the payload event until the next
 * network event arrives, and after release that reference retains almost nothing.
 * <p>
 * {@link TransactionPayloadEventData#getUncompressedEvents()} is kept non-null-but-empty: the
 * connector's EventDeserializer iterates it right after deserialization (to register inner
 * TABLE_MAPs in its own table-map cache, which only events outside payloads ever need) and would
 * NPE on null.
 */
public class MaxwellTransactionPayloadEventData extends TransactionPayloadEventData {
	private EventDeserializer eventDeserializer;
	private ByteArrayInputStream stream;

	public MaxwellTransactionPayloadEventData(byte[] decompressed, EventDeserializer.CompatibilityMode... compatibilityModes) {
		// A single deserializer instance handles the whole payload so TABLE_MAP events register
		// their column metadata before the row events that reference them. It must be fresh per
		// payload: a shared instance would accumulate table-map entries forever. Inner events
		// carry no checksum, which matches the default (NONE) on a fresh deserializer.
		this.eventDeserializer = new EventDeserializer();
		if (compatibilityModes.length > 0) {
			this.eventDeserializer.setCompatibilityMode(
				compatibilityModes[0],
				Arrays.copyOfRange(compatibilityModes, 1, compatibilityModes.length)
			);
		}
		this.stream = new ByteArrayInputStream(decompressed);

		setUncompressedEvents(new ArrayList<>());
	}

	/**
	 * Parses and returns the next inner event of the transaction, or null once exhausted.
	 * Single-pass: after the end is reached, the decompressed buffer and parser are dropped
	 * and further calls return null.
	 */
	public Event nextInnerEvent() throws IOException {
		if (stream == null) {
			return null;
		}

		Event event = eventDeserializer.nextEvent(stream);
		if (event == null) {
			eventDeserializer = null;
			stream = null;
		}
		return event;
	}
}
