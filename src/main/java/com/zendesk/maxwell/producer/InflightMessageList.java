package com.zendesk.maxwell.producer;
/* respresents a list of inflight messages -- stuff being sent over the
   network, that may complete in any order.  Allows for only bumping
   the binlog position upon completion of the oldest outstanding item.

   Assumes .addInflight(position) will be call monotonically.
   */

import com.zendesk.maxwell.BinlogPosition;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;

public class InflightMessageList {
	class InflightMessage {
		public final BinlogPosition position;
		public boolean isComplete;
		InflightMessage(BinlogPosition position) {
			this.position = position;
			this.isComplete = false;
		}
	}

	private LinkedList<InflightMessage> list;
	private HashMap<String, InflightMessage> hash;

	public InflightMessageList() {
		this.list = new LinkedList<>();
		this.hash = new HashMap<>();
	}

	public void addMessage(BinlogPosition p) {
		InflightMessage m = new InflightMessage(p);
		this.list.add(m);
		this.hash.put(p.toString(), m);
	}

	/* returns the position that stuff is complete up to, or null if there were no changes */
	public BinlogPosition completeMessage(BinlogPosition p) {
		InflightMessage m = hash.get(p.toString());
		assert(m != null);

		m.isComplete = true;

		BinlogPosition completeUntil = null;
		ListIterator<InflightMessage> iterator = this.list.listIterator();
		while ( iterator.hasNext() ) {
			InflightMessage msg = iterator.next();
			if ( !msg.isComplete )
				break;

			completeUntil = msg.position;
			iterator.remove();
			this.hash.remove(msg.position.toString());
		}

		return completeUntil;
	}

	public int size() {
		return list.size();
	}
}
