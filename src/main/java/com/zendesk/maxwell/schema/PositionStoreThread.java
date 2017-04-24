package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.MaxwellContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.util.RunLoopProcess;

public class PositionStoreThread extends RunLoopProcess implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(PositionStoreThread.class);
	private BinlogPosition position; // in memory position
	private BinlogPosition storedPosition; // position as flushed to storage
	private final MysqlPositionStore store;
	private MaxwellContext context;
	private Exception exception;
	private Thread thread;

	public PositionStoreThread(MysqlPositionStore store, MaxwellContext context) {
		this.store = store;
		this.context = context;
	}

	public void start() {
		this.thread = new Thread(this, "Position Flush Thread");
		this.thread.setDaemon(true);
		thread.start();
	}

	@Override
	public void run() {
		try {
			runLoop();
		} catch ( Exception e ) {
			this.exception = e;
			context.terminate(e);
		} finally {
			this.taskState.stopped();
		}
	}

	@Override
	public void requestStop() {
		super.requestStop();
		thread.interrupt();
	}

	@Override
	protected void beforeStop() {
		if ( exception == null ) {
			LOGGER.info("Storing final position: " + position);
			try {
				store.set(position);
			} catch ( Exception e ) {
				LOGGER.error("error storing final position: " + e);
			}
		}
	}

	public void heartbeat() throws Exception {
		store.heartbeat();
	}

	private boolean shouldHeartbeat(BinlogPosition heartbeatPosition, BinlogPosition currentPosition) {
		if ( currentPosition == null )
			return true;
		if ( heartbeatPosition == null )
			return true;
		if ( !heartbeatPosition.getFile().equals(currentPosition.getFile()) )
			return true;
		if ( currentPosition.getOffset() - heartbeatPosition.getOffset() > 1000 ) {
			return true;
		}
		return false;
	}

	BinlogPosition lastHeartbeatPosition = null; // last position we sent a heartbeat to
	public void work() throws Exception {
		BinlogPosition newPosition = position;

		if ( newPosition != null && newPosition.newerThan(storedPosition) ) {
			store.set(newPosition);
			storedPosition = newPosition;
		}

		try { Thread.sleep(1000); } catch (InterruptedException e) { }

		if ( shouldHeartbeat(lastHeartbeatPosition, position) )  {
			store.heartbeat();
			lastHeartbeatPosition = position;
		}
	}

	public synchronized void setPosition(BinlogPosition p) {
		if ( position == null || p.newerThan(position) )
			position = p;
	}

	public synchronized BinlogPosition getPosition() throws SQLException {
		if ( position != null )
			return position;

		position = store.get();

		return position;
	}
}

