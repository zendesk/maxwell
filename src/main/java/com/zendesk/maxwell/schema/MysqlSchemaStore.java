package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.zendesk.maxwell.schema.SchemaScavenger.LOGGER;

public class MysqlSchemaStore extends AbstractSchemaStore implements SchemaStore {
	private MysqlSavedSchema savedSchema;

	public MysqlSchemaStore(MaxwellContext context) {
		super(context);
	}

	public Schema getSchema() throws SchemaStoreException {
		if ( savedSchema == null )
			savedSchema = restoreOrCaptureSchema();
		return savedSchema.getSchema();
	}

	private MysqlSavedSchema restoreOrCaptureSchema() throws SchemaStoreException {
		try ( Connection conn = context.getMaxwellConnection() ) {
			MysqlSavedSchema savedSchema = MysqlSavedSchema.restore(this.context, this.context.getInitialPosition());
			if ( savedSchema == null ) {
				Schema capturedSchema = captureSchema();
				savedSchema = new MysqlSavedSchema(context, capturedSchema, this.context.getInitialPosition());
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
		/* TODO: replay mode should trigger a null schema-store */
		if ( this.context.getReplayMode() )
			return;

		try (Connection c = this.context.getMaxwellConnection()) {
			this.savedSchema = this.savedSchema.createDerivedSchema(updatedSchema, p, changes);
			this.savedSchema.save(c);
		}
	}
}
