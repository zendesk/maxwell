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

public class MysqlSchemaStore implements SchemaStore {
	private final MaxwellContext context;
	private MysqlSavedSchema savedSchema;

	public MysqlSchemaStore(MaxwellContext context) {
		this.context = context;
	}

	public void initPosition(BinlogPosition position) throws SchemaStoreException, InvalidSchemaError {
		try ( Connection conn = context.getMaxwellConnection() ) {
			savedSchema = MysqlSavedSchema.restore(conn, this.context);
			if ( savedSchema == null ) {
				// capture, save.
			}
		} catch (SQLException e) {
			throw new SchemaStoreException(e);
		}
	}

	public Schema getSchema() {
		return savedSchema.getSchema();
	}


	public List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, BinlogPosition position) throws SchemaStoreException, InvalidSchemaError {
		List<SchemaChange> changes = SchemaChange.parse(currentDatabase, sql);

		if ( changes == null || changes.size() == 0 )
			return new List<>();

		ArrayList<ResolvedSchemaChange> resolvedSchemaChanges = new ArrayList<>();

		Schema updatedSchema = getSchema();

		for ( SchemaChange change : changes ) {
			if ( !change.isBlacklisted(this.context.getFilter()) ) {
				ResolvedSchemaChange resolved = change.resolve(updatedSchema);
				if ( resolved != null ) {
					resolved.apply(updatedSchema);

					resolvedSchemaChanges.add(resolved);
				}
			} else {
				LOGGER.debug("ignoring blacklisted schema change");
			}
		}

		if ( resolvedSchemaChanges.size() > 0 ) {
			LOGGER.info("storing schema @" + position + " after applying \"" + sql.replace('\n', ' ') + "\"");

			saveSchema(updatedSchema, resolvedSchemaChanges, position);
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
