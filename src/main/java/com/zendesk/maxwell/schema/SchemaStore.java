package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

import java.util.List;

public interface SchemaStore {
	/**
	 * Retrieve the current schema
	 *
	 * If no Stored schema is found, this method should capture and save a snapshot
	 * of the current mysql schema.
	 * @return The schema, either retrieved from storage or captured.
	 */
	Schema getSchema() throws SchemaStoreException;

	/**
	 * Process a DDL statement
	 *
	 * Parse the given SQL, applying the changes to the schema.  Returns
	 * a list of ResolvedSchemaChange objects, representing the DDL that was applied (if any)
	 * @param sql The SQL of the DDL statement
	 * @param currentDatabase The "contextual database" of the DDL statement
	 * @param position The position of the DDL statement
	 * @return A list of the schema changes parsed from the SQL.
	 */
	List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, BinlogPosition position) throws SchemaStoreException, InvalidSchemaError;

	/**
	 * Clone the store, swapping out the serverID and position.  Used for master recovery.
	 *
	 * @param serverID The server_id to swap to
	 * @param position The binlog position to swap to
	 * @param readOnly should the cloned store be read-only?
	 * @return a new SchemaStore object
	 */
	SchemaStore clone(Long serverID, BinlogPosition position, boolean readOnly);
}
