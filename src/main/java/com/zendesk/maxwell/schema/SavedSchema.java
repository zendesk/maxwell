package com.zendesk.maxwell.schema;

import java.util.List;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

public interface SavedSchema {
	public BinlogPosition getPosition();
	public Schema getSchema();

	/**
	 * Process a DDL statement
	 *
	 * Parse the given SQL, applying the changes to supplied schema parameter, and returning
	 * a list of ResolvedSchemaChange objects (which may be serialized to JSON).
	 * Should modify the internal schema.
	 *
	 * @param schema The schema at the time the DDL statement was encountered
	 * @param sql The SQL of the DDL statement
	 * @param currentDatabase The "contextual database" of the DDL statement
	 * @param position The position of the DDL statement
	 * @return A list of the schema changes parsed from the SQL.
	 */
	List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, BinlogPosition position) throws SchemaStoreException, InvalidSchemaError;
}
