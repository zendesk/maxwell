package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;

/**
 * A {@link BinaryLogClient} whose GTID tracking keeps working when MySQL runs with
 * {@code binlog_transaction_compression=ON}.
 * <p>
 * The stock client commits the pending gtid into its set when it sees the transaction's XID
 * (or COMMIT query) event. With compression those events are inside a single TRANSACTION_PAYLOAD
 * event that {@code updateGtidSet} has no case for, so the pending gtid is never committed:
 * {@link #getGtidSet()} freezes at its initial value, every position Maxwell stamps on rows
 * carries that frozen set, and {@code BinlogPosition.newerThan} (a gtid-set containment check)
 * rejects every position update — Maxwell makes no durable progress and a restart replays
 * everything since startup.
 * <p>
 * A transaction payload always wraps exactly one complete transaction, so its arrival <em>is</em>
 * the commit: translate it into the XID event the stock bookkeeping expects. The XID branch of
 * {@code updateGtidSet} never reads the event argument, so a bare synthetic event suffices.
 */
public class MaxwellBinaryLogClient extends BinaryLogClient {
	private static final Event SYNTHETIC_XID_EVENT = syntheticXidEvent();

	public MaxwellBinaryLogClient(String hostname, int port, String username, String password) {
		super(hostname, port, username, password);
	}

	@Override
	protected void updateGtidSet(Event event) {
		if ( event.getHeader().getEventType() == EventType.TRANSACTION_PAYLOAD ) {
			super.updateGtidSet(SYNTHETIC_XID_EVENT);
		} else {
			super.updateGtidSet(event);
		}
	}

	private static Event syntheticXidEvent() {
		EventHeaderV4 header = new EventHeaderV4();
		header.setEventType(EventType.XID);
		return new Event(header, null);
	}
}
