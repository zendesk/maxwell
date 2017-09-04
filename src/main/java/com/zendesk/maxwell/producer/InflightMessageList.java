package com.zendesk.maxwell.producer;
/* respresents a list of inflight messages -- stuff being sent over the
   network, that may complete in any order.  Allows for only bumping
   the binlog position upon completion of the oldest outstanding item.

   Assumes .addInflight(position) will be call monotonically.
   */

import com.zendesk.maxwell.replication.Position;

import java.util.Iterator;
import java.util.LinkedHashMap;

public class InflightMessageList {

	class InflightMessage {
		public final Position position;
		public boolean isComplete;

		InflightMessage(Position p) {
			this.position = p;
			this.isComplete = false;
		}
	}

	private static final int INIT_CAPACITY = 1000;

	private final LinkedHashMap<Position, InflightMessage> linkedMap;
	private final int capacity;
	private volatile boolean isFull;

	public InflightMessageList() {
		this(INIT_CAPACITY);
	}

	public InflightMessageList(int capacity) {
		this.linkedMap = new LinkedHashMap<>();
		this.capacity = capacity;
	}

	public void addMessage(Position p) throws InterruptedException {
		synchronized (this.linkedMap) {
			while (isFull) {
				this.linkedMap.wait();
			}

			InflightMessage m = new InflightMessage(p);
			this.linkedMap.put(p, m);

			if (linkedMap.size() >= capacity) {
				isFull = true;
			}
		}
	}

	/* returns the position that stuff is complete up to, or null if there were no changes */
	public Position completeMessage(Position p) {
		synchronized (this.linkedMap) {
			InflightMessage m = this.linkedMap.get(p);
			assert(m != null);

			m.isComplete = true;

			Position completeUntil = null;
			Iterator<InflightMessage> iterator = this.linkedMap.values().iterator();

			while ( iterator.hasNext() ) {
				InflightMessage msg = iterator.next();
				if ( !msg.isComplete ) {
					break;
				}

				completeUntil = msg.position;
				iterator.remove();
			}

			if (isFull && linkedMap.size() < capacity) {
				isFull = false;
				this.linkedMap.notify();
			}

			return completeUntil;
		}
	}

	public int size() {
		return linkedMap.size();
	}
}
