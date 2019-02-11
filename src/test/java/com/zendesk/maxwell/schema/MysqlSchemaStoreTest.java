package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellTestWithIsolatedServer;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MysqlSchemaStoreTest extends MaxwellTestWithIsolatedServer {

	@Test
	public void testGetSchemaID() throws Exception {
		MaxwellContext context = buildContext();

		// store should record schema changes
		{
			Position p = new Position(new BinlogPosition(0, "mysql.1234"), 1);
			MysqlSchemaStore schemaStore = new MysqlSchemaStore(context, p);
			assertThat(schemaStore.getSchemaID(), is(1L));

			String sql = "CREATE DATABASE `testdb`;";
			String db = "testdb";
			Position pos2 = new Position(new BinlogPosition(1, "mysql.1234"), 1);
			schemaStore.processSQL(sql, db, pos2);
			assertThat(schemaStore.getSchemaID(), is(2L));
		}

		// re-created store should restore schema with changes
		{
			Position p = new Position(new BinlogPosition(2, "mysql.1234"), 1);
			MysqlSchemaStore schemaStore = new MysqlSchemaStore(context, p);
			assertThat(schemaStore.getSchemaID(), is(2L));
		}
	}

	@Test
	public void testSavedSchemaChain() throws Exception {
		MaxwellContext context = buildContext();

		Position p = new Position(new BinlogPosition(0, "mysql.1234"), 1);
		MysqlSchemaStore store = new MysqlSchemaStore(context, p);
		store.getSchema();
		assertThat(store.savedSchema.getSchemaID(), is(1L));
		assertThat(store.savedSchema.getSchemaChainLength(), is(0));

		for (int i = 1; i <= 10; i++) {
			String db = String.format("testdb_%d", i);
			Position q = new Position(new BinlogPosition(i, "mysql.1234"), 1);
			store.processSQL(String.format("CREATE DATABASE `%s`;", db), db, q);
			assertThat(store.savedSchema.getSchemaID(), is((long)i + 1));
			assertThat(store.savedSchema.getSchemaChainLength(), is(i));
		}
	}

}
