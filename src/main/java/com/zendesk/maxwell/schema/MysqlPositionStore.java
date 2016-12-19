package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import com.zendesk.maxwell.recovery.RecoveryInfo;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.errors.DuplicateProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import snaq.db.ConnectionPool;

public class MysqlPositionStore {
	static final Logger LOGGER = LoggerFactory.getLogger(MysqlPositionStore.class);
	private final Long serverID;
	private String clientID;
	private final ConnectionPool connectionPool;

	public MysqlPositionStore(ConnectionPool pool, Long serverID, String clientID) {
		this.connectionPool = pool;
		this.serverID = serverID;
		this.clientID = clientID;
	}

	public void set(BinlogPosition newPosition) throws SQLException {
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

		try( Connection c = connectionPool.getConnection() ){
			PreparedStatement s = c.prepareStatement(sql);

			LOGGER.debug("Writing binlog position to " + c.getCatalog() + ".positions: " + newPosition + ", last heartbeat read: " + heartbeat);
			s.setLong(1, serverID);
			s.setString(2, newPosition.getFile());
			s.setLong(3, newPosition.getOffset());
			s.setString(4, clientID);
			s.setString(5, newPosition.getFile());
			s.setLong(6, newPosition.getOffset());

			s.execute();
		}
	}

	public synchronized void heartbeat() throws Exception {
		try ( Connection c = connectionPool.getConnection() ) {
			heartbeat(c);
		}
	}

	/*
	 * the heartbeat system performs two functions:
	 * 1 - it leaves pointers in the binlog in order to facilitate master recovery
	 * 2 - it detects duplicate maxwell processes configured with the same client_id, aborting if we detect a dupe.
	 */

	private Long lastHeartbeat = null;

	private Long insertHeartbeat(Connection c, Long thisHeartbeat) throws SQLException, DuplicateProcessException {
		String heartbeatInsert = "insert into `heartbeats` set `heartbeat` = ?, `server_id` = ?, `client_id` = ?";

		PreparedStatement s = c.prepareStatement(heartbeatInsert);
		s.setLong(1, thisHeartbeat);
		s.setLong(2, serverID);
		s.setString(3, clientID);

		try {
			s.execute();
			return thisHeartbeat;
		} catch ( MySQLIntegrityConstraintViolationException e ) {
			throw new DuplicateProcessException("Found heartbeat row for client,position while trying to insert.  Is another maxwell running?");
		}
	}

	private void heartbeat(Connection c) throws SQLException, DuplicateProcessException, InterruptedException {
		Long thisHeartbeat = System.currentTimeMillis();

		if ( lastHeartbeat == null ) {
			PreparedStatement s = c.prepareStatement("SELECT `heartbeat` from `heartbeats` where server_id = ? and client_id = ?");
			s.setLong(1, serverID);
			s.setString(2, clientID);

			ResultSet rs = s.executeQuery();
			if ( !rs.next() ) {
				insertHeartbeat(c, thisHeartbeat);
				lastHeartbeat = thisHeartbeat;
				return;
			} else {
				lastHeartbeat = rs.getLong("heartbeat");
			}
		}

		String heartbeatUpdate = "update `heartbeats` set `heartbeat` = ? where `server_id` = ? and `client_id` = ? and `heartbeat` = ?";

		PreparedStatement s = c.prepareStatement(heartbeatUpdate);
		s.setLong(1, thisHeartbeat);
		s.setLong(2, serverID);
		s.setString(3, clientID);
		s.setLong(4, lastHeartbeat);

		LOGGER.debug("writing heartbeat: " + thisHeartbeat + " (last heartbeat written: " + lastHeartbeat + ")");
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

	public BinlogPosition get() throws SQLException {
		try ( Connection c = connectionPool.getConnection() ) {
			PreparedStatement s = c.prepareStatement("SELECT * from `positions` where server_id = ? and client_id = ?");
			s.setLong(1, serverID);
			s.setString(2, clientID);

			ResultSet rs = s.executeQuery();
			if ( !rs.next() )
				return null;

			return new BinlogPosition(rs.getLong("binlog_position"), rs.getString("binlog_file"));
		}
	}

	/**
	 * grabs a position from a different server_id
	 */

	public RecoveryInfo getRecoveryInfo() throws SQLException {
		try ( Connection c = connectionPool.getConnection() ) {
			return getRecoveryInfo(c);
		}
	}

	private RecoveryInfo getRecoveryInfo(Connection c) throws SQLException {
		RecoveryInfo info = null;

		PreparedStatement s = c.prepareStatement("SELECT * from `positions` where client_id = ?");
		s.setString(1, clientID);
		ResultSet rs = s.executeQuery();


		while ( rs.next() ) {
			Long server_id = rs.getLong("server_id");
			BinlogPosition position = BinlogPosition.at(rs.getLong("binlog_position"), rs.getString("binlog_file"));
			Long last_heartbeat_read = rs.getLong("last_heartbeat_read");

			if ( rs.wasNull() ) {
				LOGGER.warn("master recovery is ignorning position with NULL heartbeat");
			} else if ( info != null ) {
				LOGGER.error("found multiple binlog positions for cluster.  Not attempting position recovery.");
				LOGGER.error("found a row for server_id: " + info.serverID);
				LOGGER.error("also found a row for server_id: " + server_id);
				return null;
			} else {
				info = new RecoveryInfo(position, last_heartbeat_read, server_id, clientID);
			}
		}
		return info;
	}

	public int delete(Long serverID, String clientID, BinlogPosition position) throws SQLException {
		try ( Connection c = connectionPool.getConnection()) {
			PreparedStatement s = c.prepareStatement(
				"DELETE from `positions` where server_id = ? and client_id = ? and binlog_file = ? and binlog_position = ?"
			);
			s.setLong(1, serverID);
			s.setString(2, clientID);
			s.setString(3, position.getFile());
			s.setLong(4, position.getOffset());
			s.execute();
			return s.getUpdateCount();
		}
	}
}
