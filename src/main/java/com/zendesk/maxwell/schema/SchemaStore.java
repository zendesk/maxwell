package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

import java.util.List;

public interface SchemaStore {
	void getSchema(BinlogPosition position) throws SchemaStoreException, InvalidSchemaError;
	List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, BinlogPosition position) throws SchemaStoreException, InvalidSchemaError;
}
