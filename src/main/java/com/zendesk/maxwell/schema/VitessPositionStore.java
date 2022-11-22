package com.zendesk.maxwell.schema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.errors.DuplicateProcessException;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.replication.vitess.Vgtid;
import com.zendesk.maxwell.util.ConnectionPool;

public class VitessPositionStore extends MysqlPositionStore {
	static final Logger LOGGER = LoggerFactory.getLogger(VitessPositionStore.class);

	public VitessPositionStore(ConnectionPool pool, String clientID) {
		super(pool, 0L, clientID, true);
	}

	@Override
	public void set(Position p) throws SQLException, DuplicateProcessException {
		if (p == null) {
			LOGGER.debug("Position is null, not persisting it");
			return;
		}

		Vgtid vgtid = p.getVgtid();
		if (vgtid == null) {
			throw new RuntimeException("Vitess position store called with a mysql position");
		}

		String sql = "INSERT INTO `positions` "
				+ "SET server_id = ?, client_id = ?, vgtid = ? "
				+ "ON DUPLICATE KEY UPDATE client_id = ?, vgtid = ?";

		connectionPool.withSQLRetry(1, (c) -> {
			try (PreparedStatement s = c.prepareStatement(sql)) {
				final String vgtidStr = vgtid.toString();
				LOGGER.debug("Writing VGTID to {}.positions: {}", c.getCatalog(), vgtidStr);

				s.setLong(1, serverID);
				s.setString(2, clientID);
				s.setString(3, vgtidStr);
				s.setString(4, clientID);
				s.setString(5, vgtidStr);

				s.execute();
			}
		});
	}

	@Override
	public long heartbeat() throws Exception {
		// Heartbeats are not supported in Vitess.
		return System.currentTimeMillis();
	}

	@Override
	protected Position positionFromResultSet(ResultSet rs) throws SQLException {
		if (!rs.next()) {
			return null;
		}

		String vgtidString = rs.getString("vgtid");
		Vgtid vgtid = Vgtid.of(vgtidString);

		return new Position(vgtid);
	}
}
