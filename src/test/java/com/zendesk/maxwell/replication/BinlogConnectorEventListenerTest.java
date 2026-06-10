package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import org.junit.Test;

import java.util.concurrent.LinkedBlockingDeque;

import static com.zendesk.maxwell.replication.MaxwellTransactionPayloadDeserializerTest.concat;
import static com.zendesk.maxwell.replication.MaxwellTransactionPayloadDeserializerTest.xidEventBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BinlogConnectorEventListenerTest {
	private static final EventDeserializer.CompatibilityMode[] COMPAT_MODES = {
		EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
		EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
		EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE
	};

	@Test
	public void testUnwrapRestampsInnerEventsWithPayloadPosition() throws Exception {
		LinkedBlockingDeque<BinlogConnectorEvent> queue = new LinkedBlockingDeque<>(20);
		MaxwellBinaryLogClient client = new MaxwellBinaryLogClient("127.0.0.1", 3306, "maxwell", "maxwell");
		client.setBinlogFilename("mysql-bin.000123");
		BinlogConnectorEventListener listener = new BinlogConnectorEventListener(
			client,
			queue,
			new NoOpMetrics(),
			new MaxwellOutputConfig()
		);

		EventHeaderV4 payloadHeader = new EventHeaderV4();
		payloadHeader.setEventType(EventType.TRANSACTION_PAYLOAD);
		payloadHeader.setEventLength(500L);
		payloadHeader.setNextPosition(12345L);
		payloadHeader.setTimestamp(99_000L);

		byte[] decompressed = concat(xidEventBytes(111L, 1000), xidEventBytes(222L, 2000));
		Event payloadEvent = new Event(payloadHeader, new MaxwellTransactionPayloadEventData(decompressed, COMPAT_MODES));

		listener.onEvent(payloadEvent);

		assertEquals(2, queue.size());

		BinlogConnectorEvent first = queue.poll();
		assertEquals(EventType.XID, first.getType());
		assertEquals(111L, ((XidEventData) first.getEvent().getData()).getXid());
		// every inner event reports the payload's on-disk [position, nextPosition]
		assertEquals(12345L - 500L, first.getPosition().getOffset());
		assertEquals(12345L, first.getNextPosition().getOffset());
		assertTrue(first.isCommitEvent());

		BinlogConnectorEvent second = queue.poll();
		assertEquals(222L, ((XidEventData) second.getEvent().getData()).getXid());
		assertEquals(12345L - 500L, second.getPosition().getOffset());
		assertEquals(12345L, second.getNextPosition().getOffset());

		// the payload event itself is never queued
		assertEquals(0, queue.size());
	}
}
