package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.schema.ddl.SchemaChange;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.zendesk.maxwell.schema.SchemaScavenger.LOGGER;

public class MysqlSchemaStore extends AbstractSchemaStore {
	private MysqlSavedSchema savedSchema;

	public MysqlSchemaStore(MaxwellContext context) {
		super(context);
	}

	public Schema getSchema(BinlogPosition position) throws SchemaStoreException, InvalidSchemaError {
		try ( Connection conn = context.getMaxwellConnection() ) {
			savedSchema = MysqlSavedSchema.restore(this.context, position);
			if ( savedSchema == null ) {
				Schema capturedSchema = captureSchema();
				savedSchema = new MysqlSavedSchema(context, capturedSchema);
				savedSchema.save(conn);
			}

			return savedSchema.getSchema();
		} catch (SQLException e) {
			throw new SchemaStoreException(e);
		}
	}

	public List<ResolvedSchemaChange> processSQL(Schema schema, String sql, String currentDatabase, BinlogPosition position) throws SchemaStoreException, InvalidSchemaError {
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

		if ( resolvedSchemaChanges.size() > 0 ) {
			LOGGER.info("storing schema @" + position + " after applying \"" + sql.replace('\n', ' ') + "\"");

			try {
				saveSchema(schema, resolvedSchemaChanges, position);
			} catch (SQLException e) {
				throw new SchemaStoreException(e);
			}
		}
		return resolvedSchemaChanges;
	}

	private void saveSchema(Schema updatedSchema, List<ResolvedSchemaChange> changes, BinlogPosition p) throws SQLException {
		if ( this.context.getReplayMode() )
			return;

		try (Connection c = this.context.getMaxwellConnection()) {
			this.savedSchema = this.savedSchema.createDerivedSchema(updatedSchema, p, changes);
			this.savedSchema.save(c);
		}
	}
}
