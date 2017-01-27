package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.util.concurrent.ArrayBlockingQueue;

public class AbstractAsyncProducerWorker extends AbstractAsyncProducer implements Runnable {

	private final ArrayBlockingQueue<RowMap> queue;

	public AbstractAsyncProducerWorker(MaxwellContext context, ArrayBlockingQueue<RowMap> queue) {
		super(context);
		this.queue = queue;
	}

	@Override
	public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {

	}


	public void run() {
		while ( true ) {
			try {
				RowMap row = queue.take();
				this.push(row);
			} catch ( Exception e ) {
				throw new RuntimeException(e);
			}
		}
	}
}
