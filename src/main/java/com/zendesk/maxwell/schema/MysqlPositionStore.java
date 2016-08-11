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
import com.zendesk.maxwell.errors.DuplicateProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import snaq.db.ConnectionPool;

public class MysqlPositionStore extends RunLoopProcess implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(MysqlPositionStore.class);
	private final Long serverID;
	private final AtomicReference<BinlogPosition> position;
	private final AtomicReference<BinlogPosition> storedPosition;
	private final AtomicBoolean run;
	private Thread thread;
	private String schemaDatabaseName;
	private String clientID;
	private final ConnectionPool connectionPool;
	private Exception exception;

	public MysqlPositionStore(ConnectionPool pool, Long serverID, String dbName, String clientID) {
		this.connectionPool = pool;
		this.serverID = serverID;
		this.schemaDatabaseName = dbName;
		this.clientID = clientID;
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
			try {
				store(position.get());
			} catch ( SQLException e ) {
				LOGGER.error("error storing final position: " + e);
			}
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
			LOGGER.error("Hit " + e.getClass().getName() + " exception in MysqlPositionStore thread.");
			this.requestStop();
			this.exception = e;
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

		heartbeat();
	}


	private void store(BinlogPosition newPosition) throws SQLException {
		if ( newPosition == null )
			return;

		Long heartbeat = newPosition.getHeartbeat();
		String lastHeartbeatSQL = heartbeat == null ? "" : "last_heartbeat_read = " + heartbeat + ", ";

		String sql = "INSERT INTO `positions` set "
				+ "server_id = ?, "
				+ "binlog_file = ?, "
				+ "binlog_position = ?, "
				+ lastHeartbeatSQL
				+ "client_id = ? "
				+ "ON DUPLICATE KEY UPDATE "
				+ lastHeartbeatSQL
				+ "binlog_file = ?, binlog_position=?";

		try( Connection c = getConnection() ){
			PreparedStatement s = c.prepareStatement(sql);

			LOGGER.debug("Writing binlog position to " + this.schemaDatabaseName + ".positions: " + newPosition);
			s.setLong(1, serverID);
			s.setString(2, newPosition.getFile());
			s.setLong(3, newPosition.getOffset());
			s.setString(4, clientID);
			s.setString(5, newPosition.getFile());
			s.setLong(6, newPosition.getOffset());

			s.execute();
			storedPosition.set(newPosition);
		}
	}

	private void heartbeat() throws Exception {
		try ( Connection c = getConnection() ) {
			heartbeat(c);
		}
	}

	/*
	 * detect duplicate maxwell processes configured with the same client_id, aborting if we detect a dupe.
	 * note that this could at some point provide the basis for doing Maxwell-HA independent of
	 * a distributed lock system, but we'd have to rework the interfaces.
	 */

	private Long lastHeartbeat = null;

	private void heartbeat(Connection c) throws SQLException, DuplicateProcessException, InterruptedException {
		if ( lastHeartbeat == null ) {
			PreparedStatement s = c.prepareStatement("SELECT `heartbeat_at` from `positions` where server_id = ? and client_id = ?");
			s.setLong(1, serverID);
			s.setString(2, clientID);

			ResultSet rs = s.executeQuery();
			if ( !rs.next() )
				return; // we haven't yet written a position, so no heartbeat is available.

			lastHeartbeat = rs.getLong("heartbeat_at");
			if ( rs.wasNull() )
				lastHeartbeat = null;
		}

		Long thisHeartbeat = System.currentTimeMillis();

		String heartbeatUpdate = "update `positions` set `heartbeat_at` = ? where `server_id` = ? and `client_id` = ?";
		if ( lastHeartbeat == null )
			heartbeatUpdate += " and `heartbeat_at` IS NULL";
		else
			heartbeatUpdate += " and `heartbeat_at` = " + lastHeartbeat;

		PreparedStatement s = c.prepareStatement(heartbeatUpdate);
		s.setLong(1, thisHeartbeat);
		s.setLong(2, serverID);
		s.setString(3, clientID);

		int nRows = s.executeUpdate();
		if ( nRows != 1 ) {
			String msg = String.format(
				"Expected a heartbeat value of %d but didn't find it.  Is another Maxwell process running with the same client_id?",
				lastHeartbeat
			);

			throw new DuplicateProcessException(msg);
		}

		lastHeartbeat = thisHeartbeat;
	}

	public synchronized void set(BinlogPosition p) {
		if ( position.get() == null || p.newerThan(position.get()) )
			position.set(p);
	}

	public BinlogPosition get() throws SQLException {
		BinlogPosition p = position.get();
		if ( p != null )
			return p;

		try ( Connection c = getConnection() ) {
			PreparedStatement s = c.prepareStatement("SELECT * from `positions` where server_id = ? and client_id = ?");
			s.setLong(1, serverID);
			s.setString(2, clientID);

			ResultSet rs = s.executeQuery();
			if ( !rs.next() )
				return null;

			return new BinlogPosition(rs.getLong("binlog_position"), rs.getString("binlog_file"));
		}
	}

	private Connection getConnection() throws SQLException {
		Connection conn = this.connectionPool.getConnection();
		conn.setCatalog(this.schemaDatabaseName);
		return conn;
	}

	public Exception getException() {
		return this.exception;
	}

}
