package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.maxwell.AbstractMaxwellTest;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;

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

		for ( String alterSQL : alters) {
			List<SchemaChange> changes = SchemaChange.parse("shard_1", alterSQL);
			for ( SchemaChange change : changes ) {
				topSchema = change.apply(topSchema);
			}
		}

		Schema bottomSchema = capturer.capture();

		List<String> diff = topSchema.diff(bottomSchema, "play-along schema", "recaptured schema");
		assertThat(StringUtils.join(diff.iterator(), "\n"), diff.size(), is(0));

	}


	@Test
	public void testAlter() throws SQLException, SchemaSyncError, IOException, InterruptedException {
		String sql[] = {
			"create table shard_1.testAlter ( id int(11) unsigned default 1, str varchar(255) )",
			"alter table shard_1.testAlter add column barbar tinyint",
			"alter table shard_1.testAlter rename to shard_1.`freedonia`",
			"rename table shard_1.`freedonia` to shard_1.ducksoup, shard_1.ducksoup to shard_1.`nananana`",
			"alter table shard_1.nananana drop column barbar",

			"create table shard_2.weird_rename ( str mediumtext )",
			"alter table shard_2.weird_rename rename to lowball", // renames to shard_1.lowball

			"create table shard_1.testDrop ( id int(11) )",
			"drop table shard_1.testDrop"

		};
		testIntegration(sql);
	}

	@Test
	public void testDrop() throws SQLException, SchemaSyncError, IOException, InterruptedException {
		String sql[] = {
			"create table shard_1.testAlter ( id int(11) unsigned default 1, str varchar(255) )",
			"drop table if exists lasdkjflaskd.laskdjflaskdj",
			"drop table shard_1.testAlter"
		};

		testIntegration(sql);
	}

}
