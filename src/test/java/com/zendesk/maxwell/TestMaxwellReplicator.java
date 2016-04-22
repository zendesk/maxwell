package com.zendesk.maxwell;

import com.google.code.or.binlog.BinlogEventV4;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.bootstrap.AsynchronousBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStore;

import java.util.concurrent.TimeUnit;

/**
 * Created by ben on 9/28/15.
 */
public class TestMaxwellReplicator extends MaxwellReplicator {
	private final BinlogPosition stopAt;
	private boolean shouldStop;

	public TestMaxwellReplicator(SchemaStore schemaStore,
								 AbstractProducer producer,
								 AbstractBootstrapper bootstrapper,
								 MaxwellContext ctx,
								 BinlogPosition start,
								 BinlogPosition stop) throws Exception {
		super(schemaStore, producer, bootstrapper, ctx, start);
		LOGGER.debug("TestMaxwellReplicator initialized from " + start + " to " + stop);
		this.stopAt = stop;
	}

	public void getEvents(AbstractProducer producer) throws Exception {
		int max_tries = 100;
		shouldStop = false;

		this.replicator.start();

		while ( true ) {
			RowInterface ri = getRow();
			if ( ri != null && !(ri instanceof RowMap) )
				continue;

			RowMap row = (RowMap) ri;

			if ( row == null && bootstrapper.isRunning() ) {
				Thread.sleep(100);
				continue;
			}
			else if ( row == null ) {
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
			if ( !bootstrapper.shouldSkip(row) && !isMaxwellRow(row) ) {
				producer.push(row);
			} else {
				bootstrapper.work(row, this.producer, this);
			}
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
