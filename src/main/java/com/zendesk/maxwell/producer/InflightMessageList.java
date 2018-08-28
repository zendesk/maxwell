package com.zendesk.maxwell.producer;
/* respresents a list of inflight messages -- stuff being sent over the
   network, that may complete in any order.  Allows for only bumping
   the binlog position upon completion of the oldest outstanding item.

   Assumes .addInflight(position) will be call monotonically.
   */

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;

import java.util.Iterator;
import java.util.LinkedHashMap;

public class InflightMessageList {

	class InflightMessage {
		public final Position position;
		public boolean isComplete;
		public final long sendTimeMS;
		public final long eventTimeMS;

		InflightMessage(Position p, long eventTimeMS) {
			this.position = p;
			this.isComplete = false;
			this.sendTimeMS = System.currentTimeMillis();
			this.eventTimeMS = eventTimeMS;
		}

		long timeSinceSendMS() {
			return System.currentTimeMillis() - sendTimeMS;
		}
	}

	private static final long INIT_CAPACITY = 1000;
	private static final double COMPLETE_PERCENTAGE_THRESHOLD = 0.9;

	private final LinkedHashMap<Position, InflightMessage> linkedMap;
	private final MaxwellContext context;
	private final long capacity;
	private final long producerAckTimeoutMS;
	private final double completePercentageThreshold;
	private volatile boolean isFull;

	public InflightMessageList(MaxwellContext context) {
		this(context, INIT_CAPACITY, COMPLETE_PERCENTAGE_THRESHOLD);
	}

	public InflightMessageList(MaxwellContext context, long capacity, double completePercentageThreshold) {
		this.context = context;
		this.producerAckTimeoutMS = context.getConfig().producerAckTimeout;
		this.completePercentageThreshold = completePercentageThreshold;
		this.linkedMap = new LinkedHashMap<>();
		this.capacity = capacity;
	}

	public void addMessage(Position p, long eventTimestampMillis) throws InterruptedException {
		synchronized (this.linkedMap) {
			while (isFull) {
				this.linkedMap.wait();
			}

			InflightMessage m = new InflightMessage(p, eventTimestampMillis);
			this.linkedMap.put(p, m);

			if (linkedMap.size() >= capacity) {
				isFull = true;
			}
		}
	}

	/* returns the position that stuff is complete up to, or null if there were no changes */
	public InflightMessage completeMessage(Position p) {
		synchronized (this.linkedMap) {
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

			if (isFull && linkedMap.size() < capacity) {
				isFull = false;
				this.linkedMap.notify();
			}

			// If the head is stuck for the length of time (configurable) and majority of the messages have completed,
			// we assume the head will unlikely get acknowledged, hence terminate Maxwell.
			// This gatekeeper is the last resort since if anything goes wrong,
			// producer should have raised exceptions earlier than this point when all below conditions are met.
			if (producerAckTimeoutMS > 0 && isFull) {
				Iterator<InflightMessage> it = iterator();
				if (it.hasNext() && it.next().timeSinceSendMS() > producerAckTimeoutMS && completePercentage() >= completePercentageThreshold) {
					context.terminate(new IllegalStateException(
							"Did not receive acknowledgement for the head of the inflight message list for " + producerAckTimeoutMS + " ms"));
				}
			}

			return completeUntil;
		}
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
