package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.zendesk.maxwell.BinlogPosition;
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

			LOGGER.debug("Writing binlog position to " + c.getCatalog() + ".positions: " + newPosition);
			s.setLong(1, serverID);
			s.setString(2, newPosition.getFile());
			s.setLong(3, newPosition.getOffset());
			s.setString(4, clientID);
			s.setString(5, newPosition.getFile());
			s.setLong(6, newPosition.getOffset());

			s.execute();
		}
	}

	public void heartbeat() throws Exception {
		try ( Connection c = connectionPool.getConnection() ) {
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
}
