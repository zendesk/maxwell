package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import snaq.db.ConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static com.zendesk.maxwell.schema.MysqlSavedSchema.restore;

public class MysqlSchemaStore extends AbstractSchemaStore implements SchemaStore {
	private final ConnectionPool maxwellConnectionPool;
	private final Position initialPosition;
	private final boolean readOnly;
	private final Filter filter;
	private Long serverID;

	private MysqlSavedSchema savedSchema;

	public MysqlSchemaStore(ConnectionPool maxwellConnectionPool,
							ConnectionPool replicationConnectionPool,
							ConnectionPool schemaConnectionPool,
							Long serverID,
							Position initialPosition,
							CaseSensitivity caseSensitivity,
							Filter filter,
							boolean readOnly) {
		super(replicationConnectionPool, schemaConnectionPool, caseSensitivity, filter);
		this.serverID = serverID;
		this.filter = filter;
		this.maxwellConnectionPool = maxwellConnectionPool;
		this.initialPosition = initialPosition;
		this.readOnly = readOnly;
	}

	public MysqlSchemaStore(MaxwellContext context, Position initialPosition) throws SQLException {
		this(
			context.getMaxwellConnectionPool(),
			context.getReplicationConnectionPool(),
			context.getSchemaConnectionPool(),
			context.getServerID(),
			initialPosition,
			context.getCaseSensitivity(),
			context.getFilter(),
			context.getReplayMode()
		);
	}

	public Schema getSchema() throws SchemaStoreException {
		if ( savedSchema == null )
			savedSchema = restoreOrCaptureSchema();
		return savedSchema.getSchema();
	}

	private MysqlSavedSchema restoreOrCaptureSchema() throws SchemaStoreException {
		try {
			MysqlSavedSchema savedSchema =
				restore(maxwellConnectionPool, serverID, caseSensitivity, initialPosition);

			if ( savedSchema == null ) {
				savedSchema = captureAndSaveSchema();
			}

			return savedSchema;
		} catch (SQLException e) {
			throw new SchemaStoreException(e);
		} catch (InvalidSchemaError e) {
			throw new SchemaStoreException(e);
		}
	}

	public MysqlSavedSchema captureAndSaveSchema() throws SQLException {
		try ( Connection conn = maxwellConnectionPool.getConnection() ) {
			MysqlSavedSchema savedSchema = new MysqlSavedSchema(serverID, caseSensitivity, captureSchema(), initialPosition);
			if (!readOnly)
				if (conn.isValid(30)) {
					savedSchema.save(conn);
				} else {
					// The capture time might be long and the conn connection might be closed already. Consulting the pool
					// again for a new connection
					Connection newConn = maxwellConnectionPool.getConnection();
					savedSchema.save(newConn);
					newConn.close();
				}
			return savedSchema;
		}
	}

	public List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, Position position) throws SchemaStoreException, InvalidSchemaError {
		List<ResolvedSchemaChange> resolvedSchemaChanges;
		try {
			resolvedSchemaChanges = resolveSQL(getSchema(), sql, currentDatabase);
		} catch (Exception e) {
			LOGGER.error("Error on bin log position " + position.toString());
			e.printStackTrace();
			throw e;
		}

		if ( resolvedSchemaChanges.size() > 0 ) {
			try {
				Long schemaId = saveSchema(getSchema(), resolvedSchemaChanges, position);
				LOGGER.info("storing schema @" + position + " after applying \"" + sql.replace('\n', ' ') + "\" to " + currentDatabase + ", new schema id is " + schemaId);
			} catch (SQLException e) {
				throw new SchemaStoreException(e);
			}
		}
		return resolvedSchemaChanges;
	}

	private Long saveSchema(Schema updatedSchema, List<ResolvedSchemaChange> changes, Position p) throws SQLException {
		if ( readOnly )
			return null;

		try (Connection c = maxwellConnectionPool.getConnection()) {
			this.savedSchema = this.savedSchema.createDerivedSchema(updatedSchema, p, changes);
			return this.savedSchema.save(c);
		}
	}

	public void clone(Long serverID, Position position) throws SchemaStoreException {
		List<ResolvedSchemaChange> empty = Collections.emptyList();

		try (Connection c = maxwellConnectionPool.getConnection()) {
			getSchema();

			MysqlSavedSchema cloned = new MysqlSavedSchema(serverID, caseSensitivity, getSchema(), position, savedSchema.getSchemaID(), empty);
			Long schemaId = cloned.save(c);
			LOGGER.info("clone schema @" + position + " based on id " + savedSchema.getSchemaID() + ", new schema id is " + schemaId);
		} catch ( SQLException e ) {
			throw new SchemaStoreException(e);
		}
	}
}
