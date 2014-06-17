package com.zendesk.exodus;

import java.util.concurrent.BlockingQueue;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;

class ExodusBinlogEventListener implements BinlogEventListener {
	private BlockingQueue<BinlogEventV4> queue; 
	public ExodusBinlogEventListener(BlockingQueue<BinlogEventV4> q) {
		this.queue = q;
	}
	public void onEvents(BinlogEventV4 event) {
		int enqueued = 0;
		while (enqueued == 0) {
			try {
				queue.put(event);
				enqueued = 1;
			} catch (InterruptedException e) {
			}

		}
	}
}