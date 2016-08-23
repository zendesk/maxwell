package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.zendesk.maxwell.BinlogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import snaq.db.ConnectionPool;

public class MysqlPositionStore {
	static final Logger LOGGER = LoggerFactory.getLogger(MysqlPositionStore.class);
	private final Long serverID;
	private String schemaDatabaseName;
	private String clientID;
	private final ConnectionPool connectionPool;

	public MysqlPositionStore(ConnectionPool pool, Long serverID, String dbName, String clientID) {
		this.connectionPool = pool;
		this.serverID = serverID;
		this.schemaDatabaseName = dbName;
		this.clientID = clientID;
	}

	public void set(BinlogPosition newPosition) throws SQLException {
		if ( newPosition == null )
			return;

		String sql = "INSERT INTO `positions` set "
				+ "server_id = ?, "
				+ "binlog_file = ?, "
				+ "binlog_position = ?, "
				+ "client_id = ? "
				+ "ON DUPLICATE KEY UPDATE binlog_file=?, binlog_position=?";
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
		}
	}

	public BinlogPosition get() throws SQLException {
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
}
