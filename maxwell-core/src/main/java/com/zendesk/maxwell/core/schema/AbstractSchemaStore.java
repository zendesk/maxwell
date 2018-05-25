package com.zendesk.maxwell.core.schema;

import com.zendesk.maxwell.core.CaseSensitivity;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellFilter;
import com.zendesk.maxwell.core.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.core.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.core.schema.ddl.SchemaChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSchemaStore {
	static final Logger LOGGER = LoggerFactory.getLogger(AbstractSchemaStore.class);
	protected final ConnectionPool replicationConnectionPool;
	protected final ConnectionPool schemaConnectionPool;
	protected final CaseSensitivity caseSensitivity;
	private final MaxwellFilter filter;

	protected AbstractSchemaStore(ConnectionPool replicationConnectionPool,
								  ConnectionPool schemaConnectionPool,
								  CaseSensitivity caseSensitivity,
								  MaxwellFilter filter) {
		this.replicationConnectionPool = replicationConnectionPool;
		this.schemaConnectionPool = schemaConnectionPool;
		this.caseSensitivity = caseSensitivity;
		this.filter = filter;
	}

	protected AbstractSchemaStore(MaxwellContext context) throws SQLException {
		this(context.getReplicationConnectionPool(), context.getSchemaConnectionPool(), context.getCaseSensitivity(), context.getFilter());
	}

	protected Schema captureSchema() throws SQLException {
		try(Connection connection = schemaConnectionPool.getConnection()) {
			LOGGER.info("Maxwell is capturing initial schema");
			SchemaCapturer capturer = new SchemaCapturer(connection, caseSensitivity);
			return capturer.capture();
		}
	}

	protected List<ResolvedSchemaChange> resolveSQL(Schema schema, String sql, String currentDatabase) throws InvalidSchemaError {
		List<SchemaChange> changes = SchemaChange.parse(currentDatabase, sql);

		if ( changes == null || changes.size() == 0 )
			return new ArrayList<>();

		ArrayList<ResolvedSchemaChange> resolvedSchemaChanges = new ArrayList<>();

		for ( SchemaChange change : changes ) {
			if ( !change.isBlacklisted(this.filter) ) {
				ResolvedSchemaChange resolved = change.resolve(schema);
				if ( resolved != null ) {
					resolved.apply(schema);

					resolvedSchemaChanges.add(resolved);
				}
			} else {
				LOGGER.debug("ignoring blacklisted schema change");
			}
		}
		return resolvedSchemaChanges;
	}
}


