package com.zendesk.maxwell.schema;

import java.sql.SQLException;
import java.sql.Connection;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.SchemaChange;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public abstract class AbstractSchemaStore {
	static final Logger LOGGER = LoggerFactory.getLogger(AbstractSchemaStore.class);
	protected final MaxwellContext context;

	protected AbstractSchemaStore(MaxwellContext context) {
		this.context = context;
	}

	protected Schema captureSchema() throws SQLException {
		try(Connection connection = context.getReplicationConnection()) {
			LOGGER.info("Maxwell is capturing initial schema");
			SchemaCapturer capturer = new SchemaCapturer(connection, this.context.getCaseSensitivity());
			return capturer.capture();
		}
	}

	protected List<ResolvedSchemaChange> resolveSQL(Schema schema, String sql, String currentDatabase) throws InvalidSchemaError {
		List<SchemaChange> changes = SchemaChange.parse(currentDatabase, sql);

		if ( changes == null || changes.size() == 0 )
			return new ArrayList<>();

		ArrayList<ResolvedSchemaChange> resolvedSchemaChanges = new ArrayList<>();

		for ( SchemaChange change : changes ) {
			if ( !change.isBlacklisted(this.context.getFilter()) ) {
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


