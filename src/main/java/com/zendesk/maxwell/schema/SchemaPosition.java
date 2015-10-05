package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import snaq.db.ConnectionPool;


// todo: rename something better
public class SchemaPosition extends RunLoopProcess implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaPosition.class);
	private final Long serverID;
	private final AtomicReference<BinlogPosition> position;
	private final AtomicReference<BinlogPosition> storedPosition;
	private final AtomicBoolean run;
	private Thread thread;
	private final ConnectionPool connectionPool;
	private SQLException exception;

	public SchemaPosition(ConnectionPool pool, Long serverID) {
		this.connectionPool = pool;
		this.serverID = serverID;
		this.position = new AtomicReference<>();
		this.storedPosition = new AtomicReference<>();
		this.exception = null;
		this.run = new AtomicBoolean(false);
	}

	public void start() {
		this.thread = new Thread(this, "Position Flush Thread");
		thread.start();
	}

	public void stopLoop() throws TimeoutException {
		this.requestStop();
		thread.interrupt();
		super.stopLoop();
	}

	@Override
	protected void beforeStop() {
		if ( exception == null ) {
			LOGGER.info("Storing final position: " + position.get());
			store(position.get());
		}
	}

	@Override
	public void run() {
		try {
			runLoop();
		} catch ( Exception e ) {
			// this code should never be hit.  There is, I suppose,
			// a design flaw in my code, but at a certain point
			// the whole inheritance + exception handling thing
			// just started to drive me nuts.
			LOGGER.error("Hit unexpected exception in SchemaPosition thread: " + e);
		}
	}

	public void work() throws Exception {
		BinlogPosition newPosition = position.get();

		if ( newPosition != null && newPosition.newerThan(storedPosition.get()) ) {
			store(newPosition);
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) { }
	}


	private void store(BinlogPosition newPosition) {
		if ( newPosition == null )
			return;

		String sql = "INSERT INTO `maxwell`.`positions` set "
				+ "server_id = ?, "
				+ "binlog_file = ?, "
				+ "binlog_position = ? "
				+ "ON DUPLICATE KEY UPDATE binlog_file=?, binlog_position=?";
		try(Connection c = connectionPool.getConnection() ){
			PreparedStatement s = c.prepareStatement(sql);

			LOGGER.debug("Writing binlog position to maxwell.positions: " + newPosition);
			s.setLong(1, serverID);
			s.setString(2, newPosition.getFile());
			s.setLong(3, newPosition.getOffset());
			s.setString(4, newPosition.getFile());
			s.setLong(5, newPosition.getOffset());

			s.execute();
			storedPosition.set(newPosition);
		} catch ( SQLException e ) {
			LOGGER.error("received SQLException while trying to save to maxwell.positions: ");
			LOGGER.error(e.getLocalizedMessage());
			this.requestStop();
			this.exception = e;
		}
	}

	public void set(BinlogPosition p) {
		position.set(p);
	}

	public void setSync(BinlogPosition p) throws SQLException {
		LOGGER.debug("syncing binlog position: " + p);
		position.set(p);
		while ( true ) {
			thread.interrupt();
			BinlogPosition s = storedPosition.get();
			if ( p.newerThan(s) ) {
				try { Thread.sleep(50); } catch (InterruptedException e) { }
				if ( exception != null )
					throw(exception);
			} else {
				break;
			}
		}
	}

	public BinlogPosition get() throws SQLException {
		BinlogPosition p = position.get();
		if ( p != null )
			return p;

		try ( Connection c = connectionPool.getConnection() ) {
			PreparedStatement s = c.prepareStatement("SELECT * from `maxwell`.`positions` where server_id = ?");
			s.setLong(1, serverID);

			ResultSet rs = s.executeQuery();
			if ( !rs.next() )
				return null;

			return new BinlogPosition(rs.getLong("binlog_position"), rs.getString("binlog_file"));
		}
	}

	public SQLException getException() {
		return this.exception;
	}

}