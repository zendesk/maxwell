package com.zendesk.maxwell.producer;
/* respresents a list of inflight messages -- stuff being sent over the
   network, that may complete in any order.  Allows for only bumping
   the binlog position upon completion of the oldest outstanding item.

   Assumes .addInflight(position) will be call monotonically.
   */

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.Semaphore;

public class InflightMessageList {
	class InflightMessage {
		public final Position position;
		public boolean isComplete;
		public final long messageID;
		public final long sendTimeMS;
		public final long eventTimeMS;
		private Long blockedHeadTimeMS;

		InflightMessage(Position p, long eventTimeMS, long messageID) {
			this.position = p;
			this.isComplete = false;
			this.sendTimeMS = System.currentTimeMillis();
			this.eventTimeMS = eventTimeMS;
			this.messageID = messageID;
		}

		long timeSinceSendMS() {
			return System.currentTimeMillis() - sendTimeMS;
		}


		private void markBlockedHead() {
			if ( this.blockedHeadTimeMS == null )
				this.blockedHeadTimeMS = System.currentTimeMillis();
		}

		private long timeAsBlockedHead() {
			if ( this.blockedHeadTimeMS == null )
				return 0L;
			else
				return System.currentTimeMillis() - this.blockedHeadTimeMS;

		}
	}

	// number of total messages we allow to be outstanding at once
	private static final int DEFAULT_CAPACITY = 10000;

	// how long before we consider the head of the queue stuck
	private final long producerAckTimeoutMS;

	private final LinkedHashMap<Position, InflightMessage> linkedMap;
	private final MaxwellContext context;
	private final Semaphore semaphore;
	private long messageCount = 0;

	public InflightMessageList(MaxwellContext context) {
		this(context, DEFAULT_CAPACITY);
	}

	public InflightMessageList(MaxwellContext context, int capacity) {
		this.context = context;
		this.producerAckTimeoutMS = context.getConfig().producerAckTimeout;
		this.linkedMap = new LinkedHashMap<>();
		this.semaphore = new Semaphore(capacity);
	}

	public long waitForSlot() throws InterruptedException {
		this.semaphore.acquire();
		return ++this.messageCount;
	}

	private synchronized InflightMessage head() {
		Iterator<InflightMessage> it = iterator();
		if ( it.hasNext() )
			return it.next();
		else
			return null;
	}

	private void checkStuckHead(long messageID) {
		// If the head is stuck for the length of time (configurable)
		// we assume the head will unlikely get acknowledged, hence terminate Maxwell.
		// This gatekeeper is the last resort since if anything goes wrong,
		// producer should have raised exceptions earlier, but sometimes kafka just goes to lunch and eats
		// a message entirely

		if (producerAckTimeoutMS ==  0)
			return;

		InflightMessage message = head();
		if ( message == null || message.messageID == messageID )
			return;

		if ( message.timeAsBlockedHead() > producerAckTimeoutMS ) {
			IllegalStateException e = new IllegalStateException(
				"Did not receive acknowledgement for the head of the inflight message list for " + producerAckTimeoutMS + " ms"
			);
			context.terminate(e);
		} else {
			message.markBlockedHead();
		}
	}

	public void freeSlot(long messageID) {
		this.semaphore.release();

		checkStuckHead(messageID);
	}

	public synchronized void addMessage(Position p, long eventTimestampMillis, long messageID) throws InterruptedException {
		InflightMessage m = new InflightMessage(p, eventTimestampMillis, messageID);
		this.linkedMap.put(p, m);
	}

	/* returns the position that stuff is complete up to, or null if there were no changes */
	public synchronized InflightMessage completeMessage(Position p) {
		InflightMessage m = this.linkedMap.get(p);
		assert(m != null);

		m.isComplete = true;

		InflightMessage completeUntil = null;
		Iterator<InflightMessage> iterator = iterator();

		while ( iterator.hasNext() ) {
			InflightMessage msg = iterator.next();
			if ( !msg.isComplete ) {
				break;
			}

			completeUntil = msg;
			iterator.remove();
		}

		return completeUntil;
	}

	public int size() {
		return linkedMap.size();
	}

	private Iterator<InflightMessage> iterator() {
		return this.linkedMap.values().iterator();
	}
}
