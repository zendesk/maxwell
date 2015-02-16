package com.zendesk.maxwell;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;


import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class SchemaStoreTest extends AbstractMaxwellTest {
	private Schema schema;
	private BinlogPosition binlogPosition;
	private SchemaStore schemaStore;

	@Before
	public void setUp() throws Exception {
		this.schema = new SchemaCapturer(server.getConnection()).capture();
		this.binlogPosition = BinlogPosition.capture(server.getConnection());
		this.schemaStore = new SchemaStore(server.getConnection(), this.schema, binlogPosition);
	}

	@Test
	public void testSave() throws SQLException, IOException, SchemaSyncError {
		this.schemaStore.save();

		SchemaStore restoredSchema = SchemaStore.restore(server.getConnection(), binlogPosition);
		assertThat(restoredSchema.getSchema().diff(this.schema, "captured schema", "restored schema").size(), is(0));
	}
}
