package com.zendesk.exodus;

import java.util.concurrent.BlockingQueue;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;

class ExodusBinlogEventListener implements BinlogEventListener {
	private final BlockingQueue<BinlogEventV4> queue;
	public ExodusBinlogEventListener(BlockingQueue<BinlogEventV4> q) {
		this.queue = q;
	}
	public void onEvents(BinlogEventV4 event) {
		boolean enqueued = false;
		while (enqueued == false) {
			try {
				queue.put(event);
				enqueued = true;
			} catch (InterruptedException e) {
			}

		}
	}
}