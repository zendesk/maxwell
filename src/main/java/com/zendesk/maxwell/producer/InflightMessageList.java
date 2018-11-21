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
	}

	// number of total messages we allow to be outstanding at once
	private static final int DEFAULT_CAPACITY = 10000;

	// number of messages we allow to be processed while the head of the queue is stuck
	private static final int DEFAULT_STUCK_HEAD_DETECTION = 3000;

	// how long before we consider the head of the queue stuck
	private final long producerAckTimeoutMS;

	private final int stuckHeadDetection;

	private final LinkedHashMap<Position, InflightMessage> linkedMap;
	private final MaxwellContext context;
	private final Semaphore semaphore;
	private long messageCount = 0;

	public InflightMessageList(MaxwellContext context) {
		this(context, DEFAULT_CAPACITY, DEFAULT_STUCK_HEAD_DETECTION);
	}

	public InflightMessageList(MaxwellContext context, int capacity, int stuckHeadDetection) {
		this.context = context;
		this.producerAckTimeoutMS = context.getConfig().producerAckTimeout;
		this.stuckHeadDetection = stuckHeadDetection;
		this.linkedMap = new LinkedHashMap<>();
		this.semaphore = new Semaphore(capacity);
	}

	public long waitForSlot() throws InterruptedException {
		this.semaphore.acquire();
		return ++this.messageCount;
	}

	public void freeSlot(long messaageID) {
		// If the head is stuck for the length of time (configurable) and majority of the messages have completed,
		// we assume the head will unlikely get acknowledged, hence terminate Maxwell.
		// This gatekeeper is the last resort since if anything goes wrong,
		// producer should have raised exceptions earlier than this point when all below conditions are met.
		if (producerAckTimeoutMS > 0) {
			Iterator<InflightMessage> it = iterator();
			if ( it.hasNext() ) {
				InflightMessage message = it.next();
				if ( message.timeSinceSendMS() > producerAckTimeoutMS
					&& messaageID - message.messageID > stuckHeadDetection)
					context.terminate(new IllegalStateException(
						"Did not receive acknowledgement for the head of the inflight message list for " + producerAckTimeoutMS + " ms"));
			}
		}

		this.semaphore.release();
	}

	public void addMessage(Position p, long eventTimestampMillis, long messageID) throws InterruptedException {
		InflightMessage m = new InflightMessage(p, eventTimestampMillis, messageID);
		this.linkedMap.put(p, m);
	}

	/* returns the position that stuff is complete up to, or null if there were no changes */
	public InflightMessage completeMessage(Position p) {
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

	private double completePercentage() {
		long completed = linkedMap.values().stream().filter(m -> m.isComplete).count();
		return completed / ((double) linkedMap.size());
	}

	private Iterator<InflightMessage> iterator() {
		return this.linkedMap.values().iterator();
	}
}
