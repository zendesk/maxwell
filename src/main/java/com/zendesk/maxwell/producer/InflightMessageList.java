package com.zendesk.maxwell.producer;
/* respresents a list of inflight messages -- stuff being sent over the
   network, that may complete in any order.  Allows for only bumping
   the binlog position upon completion of the oldest outstanding item.

   Assumes .addInflight(position) will be call monotonically.
   */

import com.codahale.metrics.Gauge;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;

import java.util.LinkedHashMap;
import java.util.Iterator;

public class InflightMessageList {
	class InflightMessage {
		public final Position position;
		public boolean isComplete;
		InflightMessage(Position p) {
			this.position = p;
			this.isComplete = false;
		}
	}

	private LinkedHashMap<Position, InflightMessage> linkedMap;

	public InflightMessageList() {
		this.linkedMap = new LinkedHashMap<>();
	}

	public synchronized void addMessage(Position p) {
		InflightMessage m = new InflightMessage(p);
		this.linkedMap.put(p, m);
	}

	/* returns the position that stuff is complete up to, or null if there were no changes */
	public synchronized Position completeMessage(Position p) {
		InflightMessage m = this.linkedMap.get(p);
		assert(m != null);

		m.isComplete = true;

		Position completeUntil = null;
		Iterator<InflightMessage> iterator = this.linkedMap.values().iterator();

		while ( iterator.hasNext() ) {
			InflightMessage msg = iterator.next();
			if ( !msg.isComplete )
				break;

			completeUntil = msg.position;
			iterator.remove();
		}

		return completeUntil;
	}

	public int size() {
		return linkedMap.size();
	}
}
