package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

import java.util.List;

public interface SchemaStore {
	void initPosition(BinlogPosition position) throws SchemaStoreException, InvalidSchemaError;
	Schema getSchema();
	List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, BinlogPosition position) throws SchemaStoreException;
}
