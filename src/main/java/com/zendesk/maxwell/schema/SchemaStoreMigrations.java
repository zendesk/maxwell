package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class SchemaStoreMigrations {
	private final Connection connection;
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);

	public SchemaStoreMigrations(Connection connection) {
		this.connection = connection;
	}

	public void upgrade() throws SQLException, SchemaSyncError {
		upgradeColumnsTable();
	}

	static final String pkTestSQL =
			"select * from information_schema.columns where table_schema = 'maxwell' "
			+ " and table_name='tables' and column_name='pk'";

	static final String pkAlterSQL =
			"alter table maxwell.tables add column pk varchar(1024) NULL default NULL";

	private void upgradeColumnsTable() throws SQLException, SchemaSyncError {
		ResultSet rs = connection.createStatement().executeQuery(pkTestSQL);
		if ( rs.next() ) {
			return;
		} else {
			LOGGER.info("upgrading maxwell.tables to include PK column");
			SchemaStore ss = verifySchemaUnchanged();
			connection.createStatement().execute(pkAlterSQL);
			overwriteSchema(ss);
		}
	}

	// If the schema change while we were down, we can't upgrade.
	// throw SchemaSyncError in this case.

	// Otherwise, return the old schema-store; we'll overwrite it definition.
	private SchemaStore verifySchemaUnchanged() throws SQLException, SchemaSyncError  {
		BinlogPosition pos = BinlogPosition.capture(connection);
		SchemaStore lastStoredSchema = SchemaStore.restore(connection, pos);

		if ( lastStoredSchema.getSchema() == null ) {
			throw new SchemaSyncError("Couldn't find last store schema at " + pos + " while try to upgrade maxwell schema!");
		}

		SchemaCapturer capturer = new SchemaCapturer(connection);
		Schema schema = capturer.capture();

		if ( !schema.equals(lastStoredSchema.getSchema()) ) {
			String diffs = StringUtils.join(
					lastStoredSchema.getSchema().diff(schema, "last captured", "now captured").toArray(),
					"\n");
			throw new SchemaSyncError("Couldn't upgrade schema.  Please drop maxwell db and try again:\n" + diffs);
		}
		lastStoredSchema.setSchema(schema);
		return lastStoredSchema;
	}

	private void overwriteSchema(SchemaStore schemaStore) throws SQLException, SchemaSyncError {
		try {
			connection.setAutoCommit(false);

			schemaStore.destroy();
			schemaStore.saveSchema(); // put save in same transaction

			connection.commit();
		} finally {
			connection.setAutoCommit(true);
		}
	}
}
