package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.util.RunLoopProcess;

public class PositionStoreThread extends RunLoopProcess implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(PositionStoreThread.class);
	private Position position; // in memory position
	private Position storedPosition; // position as flushed to storage
	private final MysqlPositionStore store;
	private MaxwellContext context;
	private Exception exception;
	private Thread thread;
	private Position lastHeartbeatSent; // last position we sent a heartbeat to

	public PositionStoreThread(MysqlPositionStore store, MaxwellContext context) {
		this.store = store;
		this.context = context;
		lastHeartbeatSent = null;
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
			try {
				storeFinalPosition();
			} catch ( Exception e ) {
				LOGGER.error("error storing final position: " + e);
			}
		}
	}

	void storeFinalPosition() throws SQLException {
		if ( position != null && !position.equals(storedPosition) ) {
			LOGGER.info("Storing final position: " + position);
			store.set(position);
		}
	}

	public void heartbeat() throws Exception {
		store.heartbeat();
	}

	private boolean shouldHeartbeat(Position heartbeatPosition, Position currentPosition) {
		if ( currentPosition == null )
			return true;
		if ( heartbeatPosition == null )
			return true;

		BinlogPosition heartbeatBinlog = heartbeatPosition.getBinlogPosition();
		BinlogPosition currentBinlog = currentPosition.getBinlogPosition();
		if ( !heartbeatBinlog.getFile().equals(currentBinlog.getFile()) )
			return true;
		if ( currentBinlog.getOffset() - heartbeatBinlog.getOffset() > 1000 ) {
			return true;
		}
		return false;
	}

	public void work() throws Exception {
		Position newPosition = position;

		if ( newPosition != null && newPosition.newerThan(storedPosition) ) {
			store.set(newPosition);
			storedPosition = newPosition;
		}

		try { Thread.sleep(1000); } catch (InterruptedException e) { }

		if ( shouldHeartbeat(lastHeartbeatSent, position) )  {
			store.heartbeat();
			lastHeartbeatSent = position;
		}
	}

	public synchronized void setPosition(Position p) {
		if ( position == null || p.newerThan(position) ) {
			position = p;
			if (storedPosition == null) {
				storedPosition = p;
			}
		}
	}

	public synchronized Position getPosition() throws SQLException {
		if ( position != null )
			return position;

		position = store.get();

		return position;
	}
}

