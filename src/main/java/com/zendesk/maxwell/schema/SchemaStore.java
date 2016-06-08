package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

import java.util.List;

public interface SchemaStore {
	/**
	 * Retrieve a Schema for the given binlog position
	 *
	 * If no Stored schema is found, this method should capture and save a snapshot
	 * of the current mysql schema.
	 * @param position The replicator's position that it wants a schema retrived for
	 * @return The found schema
	 */
	Schema getSchema(BinlogPosition position) throws SchemaStoreException, InvalidSchemaError;

	/**
	 * Process a DDL statement
	 *
	 * Parse the given SQL, applying the changes to supplied schema parameter, and returning
	 * a list of ResolvedSchemaChange objects (which may be serialized to JSON).
	 * @param schema The schema at the time the DDL statement was encountered
	 * @param sql The SQL of the DDL statement
	 * @param currentDatabase The "contextual database" of the DDL statement
	 * @param position The position of the DDL statement
	 * @return A list of the schema changes parsed from the SQL.
	 */
	List<ResolvedSchemaChange> processSQL(Schema schema, String sql, String currentDatabase, BinlogPosition position) throws SchemaStoreException, InvalidSchemaError;
}
