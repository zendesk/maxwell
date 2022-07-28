package com.zendesk.maxwell.replay;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.ddl.*;
import com.zendesk.maxwell.util.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author udyr@shlaji.com
 */
public class ReplayBinlogStore extends AbstractSchemaStore implements SchemaStore {
	private static final Logger LOGGER = LoggerFactory.getLogger(com.zendesk.maxwell.replay.ReplayBinlogStore.class);

	private final ConnectionPool schemaConnectionPool;
	private final CaseSensitivity caseSensitivity;
	private final Filter filter;
	private Schema maxwellOnlySchema;
	private ReplayConfig config;

	public ReplayBinlogStore(ConnectionPool schemaConnectionPool, CaseSensitivity caseSensitivity, ReplayConfig config) {
		super(schemaConnectionPool, schemaConnectionPool, caseSensitivity, config.filter);
		this.filter = config.filter;
		this.schemaConnectionPool = schemaConnectionPool;
		this.caseSensitivity = caseSensitivity;
		this.config = config;
	}

	@Override
	public Schema getSchema() throws SchemaStoreException {
		if (maxwellOnlySchema != null) {
			return maxwellOnlySchema;
		}

		synchronized (com.zendesk.maxwell.replay.ReplayBinlogStore.class) {
			if (maxwellOnlySchema != null) {
				return maxwellOnlySchema;
			}

			try (Connection conn = schemaConnectionPool.getConnection()) {
				SchemaCapturer capturer = new SchemaCapturer(conn, caseSensitivity);
				maxwellOnlySchema = capturer.capture();
			} catch (SQLException e) {
				throw new SchemaStoreException(e);
			}
		}

		return maxwellOnlySchema;
	}

	@Override
	public List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, Position position) throws SchemaStoreException {
		List<ResolvedSchemaChange> resolvedSchemaChanges;
		try {
			resolvedSchemaChanges = resolveSQL(getSchema(), sql, currentDatabase);
		} catch (Exception e) {
			LOGGER.error("Error on bin log position " + position.toString(), e);
			throw e;
		}
		return resolvedSchemaChanges;
	}

	@Override
	protected List<ResolvedSchemaChange> resolveSQL(Schema schema, String sql, String currentDatabase) {
		List<SchemaChange> changes = SchemaChange.parse(currentDatabase, sql);

		if (changes == null || changes.size() == 0) {
			return new ArrayList<>();
		}

		ArrayList<ResolvedSchemaChange> resolvedSchemaChanges = new ArrayList<>();
		for (SchemaChange change : changes) {
			ResolvedSchemaChange resolvedSchemaChange = resolvedSchemaChange(change, schema);
			if (resolvedSchemaChange != null) {
				resolvedSchemaChanges.add(resolvedSchemaChange);
			}
		}
		return resolvedSchemaChanges;
	}

	private ResolvedSchemaChange resolvedSchemaChange(SchemaChange schemaChange, Schema schema) {
		try {
			if (schemaChange.isBlacklisted(this.filter)) {
				LOGGER.debug("ignoring blacklisted schema change");
				return null;
			}
			ResolvedSchemaChange resolved = schemaChange.resolve(schema);
			if (resolved != null && resolved.shouldOutput(filter)) {
				resolved.apply(schema);
				return resolved;
			}
		} catch (Exception e) {
			if (shouldOutput(schemaChange)) {
				throw new RuntimeException("resolve schema failed, " + e.getMessage(), e);
			}
			LOGGER.warn("resolve schema failed, ignore: {}", e.getMessage());
		}
		return null;
	}

	private boolean shouldOutput(SchemaChange schemaChange) {
		if (schemaChange.isBlacklisted(filter)) {
			return false;
		} else if (schemaChange instanceof DatabaseAlter) {
			String database = ((DatabaseAlter) schemaChange).database;
			return !config.databaseName.equals(database) && Filter.includes(filter, database, "");
		} else if (schemaChange instanceof DatabaseCreate) {
			String database = ((DatabaseCreate) schemaChange).database;
			return !config.databaseName.equals(database) && Filter.includes(filter, database, "");
		} else if (schemaChange instanceof DatabaseDrop) {
			String database = ((DatabaseDrop) schemaChange).database;
			return !config.databaseName.equals(database) && Filter.includes(filter, database, "");
		} else if (schemaChange instanceof TableAlter) {
			String database = ((TableAlter) schemaChange).database;
			return !config.databaseName.equals(database) && Filter.includes(filter, database, ((TableAlter) schemaChange).table);
		} else if (schemaChange instanceof TableCreate) {
			String database = ((TableCreate) schemaChange).database;
			return !config.databaseName.equals(database) && Filter.includes(filter, database, ((TableCreate) schemaChange).table);
		} else if (schemaChange instanceof TableDrop) {
			String database = ((TableDrop) schemaChange).database;
			return !config.databaseName.equals(database) && Filter.includes(filter, database, ((TableDrop) schemaChange).table);
		}
		return false;
	}

	@Override
	public Long getSchemaID() throws SchemaStoreException {
		return 0L;
	}
}
