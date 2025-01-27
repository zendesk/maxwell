package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.util.ConnectionPool;
import com.zendesk.maxwell.util.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlSchemaCompactor extends RunLoopProcess {
	static final Logger LOGGER = LoggerFactory.getLogger(MysqlSchemaCompactor.class);

	private final ConnectionPool maxwellConnectionPool;
	private final String clientID;
	private final Long serverID;
	private final CaseSensitivity sensitivity;
	private final int maxDeltas;

	public MysqlSchemaCompactor(
			int maxDeltas,
			ConnectionPool maxwellConnectionPool,
			String clientID,
			Long serverID,
			CaseSensitivity sensitivity
	) {
		this.maxDeltas = maxDeltas;
		this.maxwellConnectionPool = maxwellConnectionPool;
		this.clientID = clientID;
		this.serverID = serverID;
		this.sensitivity = sensitivity;
	}

	@Override
	protected void work() throws Exception {
		try {
			doWork();
			Thread.sleep(5000);
		} catch ( InterruptedException e ) {
		} catch ( SQLException e ) {
			LOGGER.error("got SQLException trying to compact", e);
		}
	}

	private String lockName() {
		return "maxwell_schema_compaction-" + this.serverID;
	}

	private boolean getLock(Connection cx) throws SQLException {
		try ( PreparedStatement s = cx.prepareStatement("SELECT GET_LOCK(?, 0)") ) {
			s.setString(1, this.lockName());
			try ( ResultSet rs = s.executeQuery() ) {
				return rs.next() && rs.getBoolean(1);
			}
		}
	}

	private void releaseLock(Connection cx) throws SQLException {
		try ( PreparedStatement s = cx.prepareStatement("SELECT RELEASE_LOCK(?)") ) {
			s.setString(1, this.lockName());
			s.execute();
		}
	}

	public void doWork() throws Exception {
		try ( Connection cx = maxwellConnectionPool.getConnection() ) {
			cx.setAutoCommit(false);
			try {
				if (getLock(cx)) {
					compact(cx);
				}
			} finally {
				cx.setAutoCommit(true);
				releaseLock(cx);
			}
		}
	}

	private boolean shouldCompact(Connection cx) throws SQLException {
		String sql = "select count(*) as count from `schemas` where `server_id` = " + this.serverID;
		try ( PreparedStatement ps = cx.prepareStatement(sql);
			  ResultSet rs = ps.executeQuery() ) {
			return rs.next() && rs.getInt("count") >= this.maxDeltas;
		}
	}

	Long lastWarnedSchemaID = null;

	private Long chooseCompactedSchemaBase(Connection cx) throws SQLException {
		if ( !shouldCompact(cx) ) {
			return null;
		}

		String schemaSql = "select id, binlog_file, binlog_position, gtid_set, 0 as last_heartbeat_read "
				+ " from `schemas` where `server_id` = " + this.serverID
				+ " order by id desc limit 1";
		final Long schemaID;
		final Position schemaPosition;
		try ( PreparedStatement ps = cx.prepareStatement(schemaSql);
			  ResultSet rs = ps.executeQuery() ) {
			if ( !rs.next() )
				return null;

			schemaID = rs.getLong("id");
			schemaPosition = MysqlPositionStore.positionFromResultSet(rs, serverID == 0);
		}

		LOGGER.debug("trying to compact schema {} @ {}", schemaID, schemaPosition);

		try ( PreparedStatement ps = cx.prepareStatement("select * from `positions` where server_id = " + serverID);
			  ResultSet positionsRS = ps.executeQuery() ) {

			while ( positionsRS.next() ) {
				Position clientPosition = MysqlPositionStore.positionFromResultSet(positionsRS, serverID == 0);
				if ( clientPosition.newerThan(schemaPosition) ) {
					LOGGER.debug("found a client @ {}, that's fine...", clientPosition);
				} else {
					if ( !schemaID.equals(lastWarnedSchemaID) ) {
						LOGGER.warn("Not compacting schema {}, client '{}' @ {} has not reached that position yet",
								schemaID,
								positionsRS.getString("client_id"),
								clientPosition);
						lastWarnedSchemaID = schemaID;
					}
					return null;
				}
			}
		}

		return schemaID;
	}

	private void compact(Connection cx) throws SQLException, InvalidSchemaError {
		if ( !shouldCompact(cx) )
			return;

		Long schemaID = chooseCompactedSchemaBase(cx);
		if ( schemaID == null)
			return;

		LOGGER.info("compacting schemas before {}", schemaID);
		try ( Statement begin = cx.createStatement();
		      Statement update = cx.createStatement();
		      Statement commit = cx.createStatement() ) {
			begin.execute("BEGIN");

			MysqlSavedSchema savedSchema = MysqlSavedSchema.restoreFromSchemaID(schemaID, cx, this.sensitivity);
			savedSchema.saveFullSchema(cx, schemaID);
			update.executeUpdate("update `schemas` set `base_schema_id` = null, `deltas` = null where `id` = " + schemaID);

			commit.execute("COMMIT");
			LOGGER.info("Committed schema compaction for {}", schemaID);
		}

		slowDeleteSchemas(cx, schemaID);
		LOGGER.info("Finished deleting old schemas prior to {}", schemaID);
	}

	private void slowDeleteSchemas(Connection cx, long newBaseSchemaID) throws SQLException {
		cx.setAutoCommit(true);

		String sql = "select * from `schemas` where id < ? and server_id = ?";
		try ( PreparedStatement ps = cx.prepareStatement(sql) ) {
			ps.setLong(1, newBaseSchemaID);
			ps.setLong(2, serverID);

			try ( ResultSet rs = ps.executeQuery() ) {
				while ( rs.next() ) {
					slowDeleteSchema(cx, rs.getLong("id"));
				}
			}
		}
	}

	private void slowDeleteSchema(Connection cx, long schemaID) throws SQLException {
		LOGGER.debug("slow deleting schema_id: {}", schemaID);
		slowDeleteFrom("columns", cx, schemaID);
		slowDeleteFrom("tables", cx, schemaID);
		slowDeleteFrom("databases", cx, schemaID);
		try ( Statement s = cx.createStatement() ) {
			s.executeUpdate("delete from `schemas` where id = " + schemaID);
		}
	}

	private static final int DELETE_SLEEP_MS = 200;
	private static final int DELETE_LIMIT = 500;

	private void slowDeleteFrom(String table, Connection cx, long schemaID) throws SQLException {
		try ( Statement s = cx.createStatement() ) {
			while ( true ) {
				int deleted = s.executeUpdate("DELETE from `" + table + "` where schema_id = " + schemaID + " LIMIT " + DELETE_LIMIT);

				if ( deleted == 0 )
					return;

				Thread.sleep(DELETE_SLEEP_MS);
			}
		} catch ( InterruptedException e ) {}
	}
}
