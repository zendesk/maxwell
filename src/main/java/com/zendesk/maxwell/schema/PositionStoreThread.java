package com.zendesk.maxwell.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.RunLoopProcess;

public class PositionStoreThread extends RunLoopProcess implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(PositionStoreThread.class);
	private BinlogPosition position; // in memory position
	private BinlogPosition storedPosition; // position as flushed to storage
	private final MysqlPositionStore store;
	private Exception exception;
	private Thread thread;

	public PositionStoreThread(MysqlPositionStore store) {
		this.store = store;
	}

	public void start() {
		this.thread = new Thread(this, "Position Flush Thread");
		thread.start();
	}

	@Override
	public void run() {
		try {
			runLoop();
		} catch ( Exception e ) {
			LOGGER.error("Hit " + e.getClass().getName() + " exception in MysqlPositionStore thread.");
			this.exception = e;
		}
	}


	public void stopLoop() throws TimeoutException {
		this.requestStop();
		thread.interrupt();
		super.stopLoop();
	}

	@Override
	protected void beforeStop() {
		if ( exception == null ) {
			LOGGER.info("Storing final position: " + position);
			try {
				store.set(position);
			} catch ( SQLException e ) {
				LOGGER.error("error storing final position: " + e);
			}
		}
	}

	public void work() throws Exception {
		BinlogPosition newPosition = position;

		if ( newPosition != null && newPosition.newerThan(storedPosition) ) {
			store.set(newPosition);
			storedPosition = newPosition;
		}

		try { Thread.sleep(1000); } catch (InterruptedException e) { }
	}

	public synchronized void setPosition(BinlogPosition p) {
		if ( position == null || p.newerThan(position) )
			position = p;
	}

	public BinlogPosition getPosition() throws SQLException {
		if ( position != null )
			return position;

		position = store.get();
		return position;
	}

	public Exception getException() {
		return this.exception;
	}
}

