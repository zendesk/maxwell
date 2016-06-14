package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.BinlogPosition;

import java.util.List;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public interface SchemaStore {
	/**
	 * Retrieve a Schema for the given binlog position
	 *
	 * If no Stored schema is found, this method should capture and save a snapshot
	 * of the current mysql schema.
	 * @param position The replicator's position that it wants a schema retrived for
	 * @return The found schema
	 */
	SavedSchema getSchema(BinlogPosition position) throws SchemaStoreException, InvalidSchemaError;
}
