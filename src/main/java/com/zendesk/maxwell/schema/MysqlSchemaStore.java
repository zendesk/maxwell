package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.schema.ddl.SchemaChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.zendesk.maxwell.schema.MysqlSavedSchema.restore;

public class MysqlSchemaStore implements SchemaStore {

	private static final Logger LOGGER = LoggerFactory.getLogger(MysqlSchemaStore.class);

	private final MaxwellContext context;
	private final Long serverID;
	private final Position initialPosition;
	private final Filter filter;
	private final boolean readOnly;

	MysqlSavedSchema savedSchema;

	public MysqlSchemaStore(MaxwellContext context,
							Long serverID,
							Position initialPosition,
							Filter filter,
							boolean readOnly) {
		this.context = context;
		this.serverID = serverID;
		this.initialPosition = initialPosition;
		this.filter = filter;
		this.readOnly = readOnly;
	}

	public MysqlSchemaStore(MaxwellContext context, Position initialPosition) throws SQLException {
		this(
				context,
				context.getServerID(),
				initialPosition,
				context.getFilter(),
				context.getReplayMode()
		);
	}

	public Schema getSchema() throws SchemaStoreException {
		if ( savedSchema == null )
			savedSchema = restoreOrCaptureSchema();

		return savedSchema.getSchema();
	}

	public Long getSchemaID() throws SchemaStoreException {
		getSchema();
		return savedSchema.getSchemaID();
	}

	private MysqlSavedSchema restoreOrCaptureSchema() throws SchemaStoreException {
		try {
			MysqlSavedSchema savedSchema =
				restore(context.getMaxwellConnectionPool(), serverID, context.getCaseSensitivity(), initialPosition);

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
		try (Connection conn = context.getMaxwellConnectionPool().getConnection()) {
			MysqlSavedSchema savedSchema = new MysqlSavedSchema(serverID, context.getCaseSensitivity(), captureSchema(), initialPosition);
			if (!readOnly)
				if (conn.isValid(30)) {
					savedSchema.save(conn);
				} else {
					// The capture time might be long and the conn connection might be closed already. Consulting the pool
					// again for a new connection
					Connection newConn = context.getMaxwellConnectionPool().getConnection();
					savedSchema.save(newConn);
					newConn.close();
				}
			return savedSchema;
		}
	}

	private Schema captureSchema() throws SQLException {
		try (Connection connection = context.getSchemaConnectionPool().getConnection()) {
			LOGGER.info("Maxwell is capturing initial schema");
			SchemaCapturer capturer = new SchemaCapturer(connection, context.getCaseSensitivity());
			return capturer.capture();
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

	private List<ResolvedSchemaChange> resolveSQL(Schema schema, String sql, String currentDatabase) throws InvalidSchemaError {
		List<SchemaChange> changes = SchemaChange.parse(currentDatabase, sql);

		if (changes == null || changes.size() == 0)
			return new ArrayList<>();

		ArrayList<ResolvedSchemaChange> resolvedSchemaChanges = new ArrayList<>();

		for (SchemaChange change : changes) {
			if (!change.isBlacklisted(this.filter)) {
				ResolvedSchemaChange resolved = change.resolve(schema);
				if (resolved != null) {
					resolved.apply(schema);

					resolvedSchemaChanges.add(resolved);
				}
			} else {
				LOGGER.debug("ignoring blacklisted schema change");
			}
		}
		return resolvedSchemaChanges;
	}

	private Long saveSchema(Schema updatedSchema, List<ResolvedSchemaChange> changes, Position p) throws SQLException {
		if ( readOnly )
			return null;

		try (Connection c = context.getMaxwellConnectionPool().getConnection()) {
			savedSchema = savedSchema.createDerivedSchema(updatedSchema, p, changes);
			Long savedSchemaId = savedSchema.save(c);
			int compacted = savedSchema.compact(
				c,
				context.getConfig().schemaChainMaxLength,
				context.getConfig().schemaChainLength
			);
			if (compacted > 0) {
				LOGGER.info("Compacted " + compacted + " schemas from chain.");
			}
			return savedSchemaId;
		}
	}

	public void clone(Long serverID, Position position) throws SchemaStoreException {
		List<ResolvedSchemaChange> empty = Collections.emptyList();

		try (Connection c = context.getMaxwellConnectionPool().getConnection()) {
			getSchema();

			MysqlSavedSchema cloned = new MysqlSavedSchema(serverID, context.getCaseSensitivity(), getSchema(), position, empty, savedSchema);
			Long schemaId = cloned.save(c);
			LOGGER.info("clone schema @" + position + " based on id " + savedSchema.getSchemaID() + ", new schema id is " + schemaId);
		} catch (SQLException e) {
			throw new SchemaStoreException(e);
		}
	}
}
