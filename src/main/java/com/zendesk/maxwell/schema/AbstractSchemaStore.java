package com.zendesk.maxwell.schema;

import java.sql.SQLException;
import java.sql.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;

public abstract class AbstractSchemaStore implements SchemaStore {
	static final Logger LOGGER = LoggerFactory.getLogger(AbstractSchemaStore.class);
	protected final MaxwellContext context;

	protected AbstractSchemaStore(MaxwellContext context) {
		this.context = context;
	}

	protected Schema captureSchema() throws SQLException {
		try(Connection connection = context.getReplicationConnection()) {
			LOGGER.info("Maxwell is capturing initial schema");
			SchemaCapturer capturer = new SchemaCapturer(connection, this.context.getCaseSensitivity());
			return capturer.capture();
		}
	}
}


