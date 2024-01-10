package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.errors.DuplicateProcessException;
import com.zendesk.maxwell.replication.Position;
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
	private BinlogPosition lastHeartbeatSentFrom; // last position we sent a heartbeat from
	private long lastHeartbeatSent;

	public PositionStoreThread(MysqlPositionStore store, MaxwellContext context) {
		this.store = store;
		this.context = context;
		lastHeartbeatSentFrom = null;
		lastHeartbeatSent = 0L;
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
	protected void beforeStop() {
		if ( exception == null ) {
			try {
				storeFinalPosition();
			} catch ( Exception e ) {
				LOGGER.error("error storing final position: " + e);
			}
		}
	}

	void storeFinalPosition() throws SQLException, DuplicateProcessException {
		if ( position != null && !position.equals(storedPosition) ) {
			LOGGER.info("Storing final position: " + position);
			store.set(position);
		}
	}

	public void heartbeat() throws Exception {
		store.heartbeat();
	}

	boolean shouldHeartbeat(Position currentPosition) {
		if ( currentPosition == null )
			return true;
		if ( lastHeartbeatSentFrom == null )
			return true;

		BinlogPosition currentBinlog = currentPosition.getBinlogPosition();
		if ( !lastHeartbeatSentFrom.getFile().equals(currentBinlog.getFile()) )
			return true;
		if ( currentBinlog.getOffset() - lastHeartbeatSentFrom.getOffset() > 3000 ) {
			return true;
		}

		long secondsSinceHeartbeat = (System.currentTimeMillis() - lastHeartbeatSent) / 1000;
		if ( secondsSinceHeartbeat >= 10 ) {
			// during quiet times, heartbeat at least every 10s
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

		if ( shouldHeartbeat(newPosition) )  {
			lastHeartbeatSent = store.heartbeat();
			if (newPosition != null) {
				lastHeartbeatSentFrom = newPosition.getBinlogPosition();
			}
		}
	}

	public synchronized void setPosition(Position p) {
		/* in order to keep full, comparable gtid sets maria makes us jump through some hoops. */
		if ( context.isMariaDB() && position != null ) {
			BinlogPosition bp = p.getBinlogPosition();
			if ( bp.getGtid() != null ) {
				bp.mergeGtids(position.getBinlogPosition().getGtidSet());
			}
			position = p;
		} else if ( position == null || p.newerThan(position) ) {
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

