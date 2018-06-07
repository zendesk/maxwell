package com.zendesk.maxwell.api.schema;

import com.zendesk.maxwell.api.schema.InvalidSchemaError;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public interface SchemaStoreSchema {
	void ensureMaxwellSchema(Connection connection, String schemaDatabaseName) throws SQLException, IOException, InvalidSchemaError;

	void upgradeSchemaStoreSchema(Connection c) throws SQLException, IOException;
}