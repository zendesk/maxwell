package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MaxwellBinaryLogClientTest {
	private static final String SERVER_UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

	private Event gtidEvent(long transactionId) {
		EventHeaderV4 header = new EventHeaderV4();
		header.setEventType(EventType.GTID);
		GtidEventData data = new GtidEventData(
			MySqlGtid.fromString(SERVER_UUID + ":" + transactionId),
			(byte) 0, 0L, 0L, 0L, 0L, 0L, 0, 0
		);
		return new Event(header, data);
	}

	private Event transactionPayloadEvent() {
		EventHeaderV4 header = new EventHeaderV4();
		header.setEventType(EventType.TRANSACTION_PAYLOAD);
		return new Event(header, new TransactionPayloadEventData());
	}

	/* With binlog_transaction_compression=ON the XID/COMMIT events that normally commit the
	   pending gtid into the client's set arrive inside the TRANSACTION_PAYLOAD event, which the
	   stock client's gtid bookkeeping has no case for -- the set would freeze at its initial
	   value and Maxwell's gtid-mode position would never advance. */
	@Test
	public void testGtidSetAdvancesAcrossCompressedTransactions() {
		MaxwellBinaryLogClient client = new MaxwellBinaryLogClient("127.0.0.1", 3306, "maxwell", "maxwell");
		client.setGtidSet(SERVER_UUID + ":1-5");

		// the GTID event arrives uncompressed and only marks the gtid as pending
		client.updateGtidSet(gtidEvent(6));
		assertEquals(SERVER_UUID + ":1-5", client.getGtidSet());

		// the compressed transaction is the commit the stock bookkeeping never sees
		client.updateGtidSet(transactionPayloadEvent());
		assertEquals(SERVER_UUID + ":1-6", client.getGtidSet());

		// and it keeps advancing on subsequent transactions
		client.updateGtidSet(gtidEvent(7));
		client.updateGtidSet(transactionPayloadEvent());
		assertEquals(SERVER_UUID + ":1-7", client.getGtidSet());
	}
}
