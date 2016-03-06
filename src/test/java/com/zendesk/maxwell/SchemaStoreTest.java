package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public class SchemaStoreTest extends MaxwellTestWithIsolatedServer {
	private Schema schema;
	private BinlogPosition binlogPosition;
	private SchemaStore schemaStore;

	String schemaSQL[] = {
		"CREATE TABLE shard_1.latin1 (id int(11), str1 varchar(255), str2 varchar(255) character set 'utf8') charset = 'latin1'",
		"CREATE TABLE shard_1.enums (id int(11), enum_col enum('foo', 'bar', 'baz'))",
		"CREATE TABLE shard_1.pks (id int(11), col2 varchar(255), col3 datetime, PRIMARY KEY(col2, col3, id))"
	};
	private MaxwellContext context;

	@Before
	public void setUp() throws Exception {
		server.executeList(schemaSQL);

		this.binlogPosition = BinlogPosition.capture(server.getConnection());
		this.context = buildContext(binlogPosition);
		this.schema = new SchemaCapturer(server.getConnection(), context.getCaseSensitivity()).capture();
		this.schemaStore = new SchemaStore(server.getConnection(this.buildContext().getConfig().databaseName), MysqlIsolatedServer.SERVER_ID, this.schema, binlogPosition, context.getConfig().databaseName);
	}

	@Test
	public void testSave() throws SQLException, IOException, InvalidSchemaError {
		this.schemaStore.save();

		SchemaStore restoredSchema = SchemaStore.restore(server.getConnection(this.buildContext().getConfig().databaseName), context);
		List<String> diff = this.schema.diff(restoredSchema.getSchema(), "captured schema", "restored schema");
		assertThat(StringUtils.join(diff, "\n"), diff.size(), is(0));
	}

	@Test
	public void testRestorePK() throws Exception {
		this.schemaStore.save();

		SchemaStore restoredSchema = SchemaStore.restore(server.getConnection(this.buildContext().getConfig().databaseName), context);
		Table t = restoredSchema.getSchema().findDatabase("shard_1").findTable("pks");

		assertThat(t.getPKList(), is(not(nullValue())));
		assertThat(t.getPKList().size(), is(3));
		assertThat(t.getPKList().get(0), is("col2"));
		assertThat(t.getPKList().get(1), is("col3"));
		assertThat(t.getPKList().get(2), is("id"));
	}

	@Test
	public void testUpgradeToFixServerIDBug() throws Exception {
		// create a couple of schemas
		this.schemaStore.save();
		Long badSchemaID = this.schemaStore.getSchemaID();

		// throw into old state
		String updateSQL[] = {"UPDATE `" + buildContext().getConfig().databaseName+ "`.`schemas` set server_id = 1"};
		server.executeList(updateSQL);

		SchemaStore restoredSchema = SchemaStore.restore(server.getConnection(this.buildContext().getConfig().databaseName), context);

		List<String> diffs = restoredSchema.getSchema().diff(this.schemaStore.getSchema(), "restored", "captured");
		assert diffs.isEmpty() : "Expected empty schema diff, got" + diffs;
	}

	@Test
	public void testMasterChange() throws Exception {
		this.schema = new SchemaCapturer(server.getConnection(), context.getCaseSensitivity()).capture();
		this.binlogPosition = BinlogPosition.capture(server.getConnection());
		String dbName = this.buildContext().getConfig().databaseName;
		this.schemaStore = new SchemaStore(server.getConnection(dbName), 5551234L, this.schema, binlogPosition, dbName);

		this.schemaStore.save();

		SchemaStore.handleMasterChange(server.getConnection(dbName), 123456L, dbName);

		ResultSet rs = server.getConnection(dbName).createStatement().executeQuery("SELECT * from `schemas`");
		assertThat(rs.next(), is(true));
		assertThat(rs.getBoolean("deleted"), is(true));

		rs = server.getConnection(dbName).createStatement().executeQuery("SELECT * from `positions`");
		assertThat(rs.next(), is(false));
	}

	@Test
	public void testRestoreMysqlDb() throws Exception {
		Database db = this.schema.findDatabase("mysql");
		String maxwellDBName = this.buildContext().getConfig().databaseName;
		this.schema.getDatabases().remove(db);
		this.schemaStore.save();
		SchemaStore restoredSchema = SchemaStore.restore(server.getConnection(maxwellDBName), context);
		assertThat(restoredSchema.getSchema().findDatabase("mysql"), is(not(nullValue())));
	}
}
