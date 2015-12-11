package com.zendesk.maxwell;

import com.google.code.or.binlog.BinlogEventV4;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;

import java.util.concurrent.TimeUnit;

/**
 * Created by ben on 9/28/15.
 */
public class TestMaxwellReplicator extends MaxwellReplicator {
	private final BinlogPosition stopAt;
	private boolean shouldStop;

	public TestMaxwellReplicator(Schema currentSchema,
								 AbstractProducer producer,
								 MaxwellContext ctx,
								 BinlogPosition start,
								 BinlogPosition stop) throws Exception {
		super(currentSchema, producer, ctx, start);
		LOGGER.debug("TestMaxwellReplicator initialized from " + start + " to " + stop);
		this.stopAt = stop;
	}

	public void getEvents(RowConsumer consumer) throws Exception {
		int max_tries = 100;
		shouldStop = false;

		this.replicator.start();

		while ( true ) {
			RowMap r = getRow();
			if (r == null) {
				if ( shouldStop ) {
					hardStop();
					return;
				}

				if (max_tries > 0) {
					max_tries--;
					continue;
				} else {
					hardStop();
					return;
				}
			}
			consumer.consume(r);
		}
	}

	private void hardStop() throws Exception {
		this.binlogEventListener.stop();
		this.replicator.stop(5, TimeUnit.SECONDS);
	}

	@Override
	protected BinlogEventV4 pollV4EventFromQueue() throws InterruptedException
	{
		BinlogEventV4 v4 = super.pollV4EventFromQueue();
		if ( v4 != null && v4.getHeader().getNextPosition() >= this.stopAt.getOffset() ) {
			shouldStop = true;
		}

		return v4;
	}
}
