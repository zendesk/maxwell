package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellTestSupport;
import com.zendesk.maxwell.MaxwellTestWithIsolatedServer;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MysqlSchemaCompactorTest extends MaxwellTestWithIsolatedServer {
	static MysqlSchemaCompactor compactor;
	static MaxwellContext context;

	@Before
	public void setupCompactor() throws Exception {
		context = buildContext();
		MaxwellTestSupport.setupSchema(server, false);

		String input[] = {
			"drop database if exists foo",
			"create database foo",
			"create table foo.bar ( i int )",
			"alter table foo.bar add column j int",
			"alter table foo.bar drop column j",
			"alter table foo.bar add column k int",
			"alter table foo.bar drop column k",
			"alter table foo.bar add column m int"
		};

		getRowsForSQL(input);

		compactor = new MysqlSchemaCompactor(
			5,
			context.getMaxwellConnectionPool(),
			"maxwell",
			context.getServerID(),
			context.getCaseSensitivity()
		);
	}

	public int getSchemaCount() throws Exception {
		ResultSet rs = this.server.query("select count(*) as c from `maxwell`.`schemas`");
		rs.next();

		return rs.getInt("c");
	}

	@Test
	public void testCompactorCompacts() throws Exception {
		int schema_count = getSchemaCount();
		assert(schema_count > 5);

		compactor.doWork();

		int new_schema_count = getSchemaCount();

		assertEquals(1, new_schema_count);
	}

	@Test
	public void testCompactorCreatesNoDifferences() throws Exception {
		MysqlSavedSchema a = MysqlSavedSchema.restore(context, context.getPosition());

		compactor.doWork();

		MysqlSavedSchema b = MysqlSavedSchema.restore(context, context.getPosition());

		List<String> diff = a.getSchema().diff(b.getSchema(), "pre-compact", "post-compact");
		assertEquals(StringUtils.join(diff, "\n"), 0, diff.size());
	}

	@Test
	public void testCompactorClearsOutRow() throws Exception {
		compactor.doWork();

		ResultSet rs = this.server.query("select * from `maxwell`.`schemas`");
		assert(rs.next());

		assertNull(rs.getObject("base_schema_id"));
		assertNull(rs.getString("deltas"));
	}
}
