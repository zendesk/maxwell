package com.zendesk.exodus.schema.ddl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.exodus.AbstractMaxwellTest;
import com.zendesk.exodus.schema.Schema;
import com.zendesk.exodus.schema.SchemaCapturer;

public class DDLIntegrationTest extends AbstractMaxwellTest {
	@Before
	public void setUp() throws Exception {
	}

	@Override
	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}

	private void testIntegration(String alters[]) throws SQLException, SchemaSyncError, IOException {
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection());
		Schema topSchema = capturer.capture();

		server.executeList(Arrays.asList(alters));

		for ( String alter : alters) {
			SchemaChange.parse(alter).apply("shard_1", topSchema);
		}

		Schema bottomSchema = capturer.capture();

		for ( String diff : topSchema.diff(bottomSchema, "post-alter schema")) {
			System.out.println(diff);
		}
		assertThat(bottomSchema.toJSON(), is(topSchema.toJSON()));

	}


	@Test
	public void testAlter() throws SQLException, SchemaSyncError, IOException {
		String sql[] = {
			"create table shard_1.testAlter ( id int(11) unsigned default 1, str varchar(255) )",
			"alter table shard_1.testAlter add column barbar tinyint",
			"alter table shard_1.testAlter rename to shard_1.`freedonia`"
		};
		testIntegration(sql);
	}

}
