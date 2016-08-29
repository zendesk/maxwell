package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import snaq.db.ConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class MysqlSchemaStore extends AbstractSchemaStore implements SchemaStore {
	private final ConnectionPool maxwellConnectionPool;
	private final BinlogPosition initialPosition;
	private final Long serverID;
	private final boolean readOnly;
	private final MaxwellFilter filter;

	private MysqlSavedSchema savedSchema;

	public MysqlSchemaStore(ConnectionPool maxwellConnectionPool,
							ConnectionPool replicationConnectionPool,
							Long serverID,
							BinlogPosition initialPosition,
							CaseSensitivity caseSensitivity,
							MaxwellFilter filter,
							boolean readOnly) {
		super(replicationConnectionPool, caseSensitivity, filter);
		this.serverID = serverID;
		this.filter = filter;
		this.maxwellConnectionPool = maxwellConnectionPool;
		this.initialPosition = initialPosition;
		this.readOnly = readOnly;
	}

	public MysqlSchemaStore(MaxwellContext context, BinlogPosition initialPosition) throws SQLException {
		this(
			context.getMaxwellConnectionPool(),
			context.getReplicationConnectionPool(),
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
		try ( Connection conn = maxwellConnectionPool.getConnection() ) {
			MysqlSavedSchema savedSchema =
				MysqlSavedSchema.restore(maxwellConnectionPool, serverID, caseSensitivity, initialPosition);

			if ( savedSchema == null ) {
				Schema capturedSchema = captureSchema();
				savedSchema = new MysqlSavedSchema(serverID, caseSensitivity, capturedSchema, initialPosition);
				if ( !readOnly )
					savedSchema.save(conn);
			}

			return savedSchema;
		} catch (SQLException e) {
			throw new SchemaStoreException(e);
		} catch (InvalidSchemaError e) {
			throw new SchemaStoreException(e);
		}
	}


	public List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, BinlogPosition position) throws SchemaStoreException, InvalidSchemaError {
		List<ResolvedSchemaChange> resolvedSchemaChanges = resolveSQL(getSchema(), sql, currentDatabase);

		if ( resolvedSchemaChanges.size() > 0 ) {
			LOGGER.info("storing schema @" + position + " after applying \"" + sql.replace('\n', ' ') + "\"");

			try {
				saveSchema(getSchema(), resolvedSchemaChanges, position);
			} catch (SQLException e) {
				throw new SchemaStoreException(e);
			}
		}
		return resolvedSchemaChanges;
	}

	private void saveSchema(Schema updatedSchema, List<ResolvedSchemaChange> changes, BinlogPosition p) throws SQLException {
		if ( readOnly )
			return;

		try (Connection c = maxwellConnectionPool.getConnection()) {
			this.savedSchema = this.savedSchema.createDerivedSchema(updatedSchema, p, changes);
			this.savedSchema.save(c);
		}
	}
}
