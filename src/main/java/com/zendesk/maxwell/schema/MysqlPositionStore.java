package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.recovery.RecoveryInfo;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.errors.DuplicateProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import snaq.db.ConnectionPool;

public class MysqlPositionStore {
	static final Logger LOGGER = LoggerFactory.getLogger(MysqlPositionStore.class);
	private static final Long DEFAULT_GTID_SERVER_ID = new Long(0);
	private final Long serverID;
	private String clientID;
	private final boolean gtidMode;
	private final ConnectionPool connectionPool;

	public MysqlPositionStore(ConnectionPool pool, Long serverID, String clientID, boolean gtidMode) {
		this.connectionPool = pool;
		this.clientID = clientID;
		this.gtidMode = gtidMode;
		if (gtidMode) {
			// we don't use server id for position store in gtid mode
			this.serverID = DEFAULT_GTID_SERVER_ID;
		} else {
			this.serverID = serverID;
		}
	}

	public void set(BinlogPosition newPosition) throws SQLException {
		if ( newPosition == null )
			return;

		Long heartbeat = newPosition.getHeartbeat();
		String lastHeartbeatSQL = heartbeat == null ? "" : "last_heartbeat_read = " + heartbeat + ", ";

		String sql = "INSERT INTO `positions` set "
				+ "server_id = ?, "
				+ "gtid_set = ?, "
				+ "binlog_file = ?, "
				+ "binlog_position = ?, "
				+ lastHeartbeatSQL
				+ "client_id = ? "
				+ "ON DUPLICATE KEY UPDATE "
				+ lastHeartbeatSQL
				+ "gtid_set = ?, binlog_file = ?, binlog_position=?";

		try( Connection c = connectionPool.getConnection() ){
			PreparedStatement s = c.prepareStatement(sql);

			LOGGER.debug("Writing binlog position to " + c.getCatalog() + ".positions: " + newPosition + ", last heartbeat read: " + heartbeat);
			s.setLong(1, serverID);
			s.setString(2, newPosition.getGtidSetStr());
			s.setString(3, newPosition.getFile());
			s.setLong(4, newPosition.getOffset());
			s.setString(5, clientID);
			s.setString(6, newPosition.getGtidSetStr());
			s.setString(7, newPosition.getFile());
			s.setLong(8, newPosition.getOffset());

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

			String gtid = gtidMode ? rs.getString("gtid_set") : null;
			return new BinlogPosition(gtid, null,
				rs.getLong("binlog_position"), rs.getString("binlog_file"), null);
		}
	}

	/**
	 * grabs a position from a different server_id
	 */

	public RecoveryInfo getRecoveryInfo(MaxwellConfig config) throws SQLException {
		try ( Connection c = connectionPool.getConnection() ) {
			return getRecoveryInfo(config, c);
		}
	}

	protected RecoveryInfo getRecoveryInfo(MaxwellConfig config, Connection c) throws SQLException {
		List<RecoveryInfo> recoveries = getAllRecoveryInfos(c);
		if (recoveries.size() == 1) {
			return recoveries.get(0);
		} else {
			for (String line: formatRecoveryFailure(config, recoveries)) {
				LOGGER.error(line);
			}
			return null;
		}
	}

	protected List<RecoveryInfo> getAllRecoveryInfos() throws SQLException {
		try ( Connection c = connectionPool.getConnection() ) {
			return getAllRecoveryInfos(c);
		}
	}

	protected List<RecoveryInfo> getAllRecoveryInfos(Connection c) throws SQLException {
		PreparedStatement s = c.prepareStatement("SELECT * from `positions` where client_id = ? order by last_heartbeat_read DESC");
		s.setString(1, clientID);
		ResultSet rs = s.executeQuery();

		ArrayList<RecoveryInfo> recoveries = new ArrayList<>();

		while ( rs.next() ) {
			Long server_id = rs.getLong("server_id");
			String gtid = gtidMode ? rs.getString("gtid_set") : null;
			BinlogPosition position = BinlogPosition.at(gtid,
				rs.getLong("binlog_position"), rs.getString("binlog_file"));
			Long last_heartbeat_read = rs.getLong("last_heartbeat_read");

			if ( rs.wasNull() ) {
				LOGGER.warn("master recovery is ignoring position with NULL heartbeat");
			} else {
				recoveries.add(new RecoveryInfo(position, last_heartbeat_read, server_id, clientID));
			}
		}
		return recoveries;
	}

	protected List<String> formatRecoveryFailure(MaxwellConfig config, List<RecoveryInfo> recoveries) {
		if (recoveries.size() == 0) {
			return Collections.singletonList("Unable to find any binlog positions in `positions` table");
		}

		ArrayList<String> result = new ArrayList<>();
		Long mostRecentMaster = recoveries.get(0).serverID;

		result.add("Found multiple binlog positions for cluster in `positions` table.  Not attempting position recovery.");
		result.add("Positions found (most recent heartbeat first):");
		for (RecoveryInfo recovery : recoveries) {
			result.add(" - " + recovery);
		}

		result.add("Most likely the first is the most recent master, in which case you should:");
		result.add("1. stop maxwell");
		result.add("2. execute: DELETE FROM " + config.databaseName + ".positions WHERE server_id <> " + mostRecentMaster + ";");
		result.add("3. restart maxwell");
		return result;
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
