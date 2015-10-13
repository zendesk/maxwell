package com.zendesk.maxwell;

import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaScavenger;
import com.zendesk.maxwell.schema.SchemaStore;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by ben on 10/12/15.
 */
public class SchemaScavengerTest extends AbstractMaxwellTest {
	String schemaSQL[] = {
		"CREATE TABLE shard_1.latin1 (id int(11), str1 varchar(255), str2 varchar(255) character set 'utf8') charset = 'latin1'",
		"CREATE TABLE shard_1.enums (id int(11), enum_col enum('foo', 'bar', 'baz'))",
		"CREATE TABLE shard_1.pks (id int(11), col2 varchar(255), col3 datetime, PRIMARY KEY(col2, col3, id))"
	};
	private Schema schema;
	private BinlogPosition binlogPosition;
	private SchemaStore schemaStore;
	private SchemaScavenger scavenger;

	@Before
	public void setUp() throws Exception {
		server.executeList(schemaSQL);
		this.schema = new SchemaCapturer(server.getConnection()).capture();
		this.binlogPosition = BinlogPosition.capture(server.getConnection());

		this.scavenger = new SchemaScavenger(buildContext().getConnectionPool());

		this.schemaStore = new SchemaStore(server.getConnection(), MysqlIsolatedServer.SERVER_ID, this.schema, binlogPosition);
		this.schemaStore.save();
	}

	private Long getCount(String sql) throws SQLException {
		ResultSet rs = server.getConnection().createStatement().executeQuery(sql);
		rs.next();
		return rs.getLong(1);
	}

	private Long countSchemaRows() throws SQLException {
		String sql = "select sum(cnt) as cnt from ( " +
			"select count(*) as cnt from maxwell.columns UNION " +
			"select count(*) as cnt from maxwell.tables UNION " +
			"select count(*) as cnt from maxwell.databases) as u";

		return getCount(sql);
	}

	@Test
	public void testDelete() throws Exception {
		Long initialCount = countSchemaRows();
		this.schemaStore.delete();
		scavenger.deleteSchemas(20000L);

		assertThat(countSchemaRows(), is(0L));
		assertThat(getCount("select count(*) from maxwell.schemas"), is(0L));

	}
}
