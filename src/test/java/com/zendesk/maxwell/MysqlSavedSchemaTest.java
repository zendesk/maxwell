package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;

public class MysqlSavedSchemaTest extends MaxwellTestWithIsolatedServer {
	private Schema schema;
	private BinlogPosition binlogPosition;
	private MysqlSavedSchema savedSchema;

	String schemaSQL[] = {
		"CREATE TABLE shard_1.latin1 (id int(11), str1 varchar(255), str2 varchar(255) character set 'utf8') charset = 'latin1'",
		"CREATE TABLE shard_1.enums (id int(11), enum_col enum('foo', 'bar', 'baz'))",
		"CREATE TABLE shard_1.pks (id int(11), col2 varchar(255), col3 datetime, PRIMARY KEY(col2, col3, id))",
		"CREATE TABLE shard_1.signed (badcol int(10) unsigned)"
	};
	private MaxwellContext context;

	@Before
	public void setUp() throws Exception {
		server.executeList(schemaSQL);

		this.binlogPosition = BinlogPosition.capture(server.getConnection());
		this.context = buildContext(binlogPosition);
		this.schema = new SchemaCapturer(server.getConnection(), context.getCaseSensitivity()).capture();
		this.savedSchema = new MysqlSavedSchema(this.context, this.schema);
	}

	@Test
	public void testSave() throws SQLException, IOException, InvalidSchemaError {
		this.savedSchema.save(context.getMaxwellConnection());

		MysqlSavedSchema restoredSchema = MysqlSavedSchema.restore(context, context.getInitialPosition());
		List<String> diff = this.schema.diff(restoredSchema.getSchema(), "captured schema", "restored schema");
		assertThat(StringUtils.join(diff, "\n"), diff.size(), is(0));
	}

	@Test
	public void testRestorePK() throws Exception {
		this.savedSchema.save(context.getMaxwellConnection());

		MysqlSavedSchema restoredSchema = MysqlSavedSchema.restore(context, context.getInitialPosition());
		Table t = restoredSchema.getSchema().findDatabase("shard_1").findTable("pks");

		assertThat(t.getPKList(), is(not(nullValue())));
		assertThat(t.getPKList().size(), is(3));
		assertThat(t.getPKList().get(0), is("col2"));
		assertThat(t.getPKList().get(1), is("col3"));
		assertThat(t.getPKList().get(2), is("id"));
	}

	@Test
	public void testMasterChange() throws Exception {
		this.schema = new SchemaCapturer(server.getConnection(), context.getCaseSensitivity()).capture();
		this.binlogPosition = BinlogPosition.capture(server.getConnection());

		MaxwellContext context = this.buildContext();
		String dbName = context.getConfig().databaseName;

		this.savedSchema = new MysqlSavedSchema(5551234L, context.getCaseSensitivity(), this.schema);

		Connection conn = context.getMaxwellConnection();
		this.savedSchema.save(conn);

		SchemaStoreSchema.handleMasterChange(conn, 123456L, dbName);

		ResultSet rs = conn.createStatement().executeQuery("SELECT * from `schemas`");
		assertThat(rs.next(), is(true));
		assertThat(rs.getBoolean("deleted"), is(true));

		rs = conn.createStatement().executeQuery("SELECT * from `positions`");
		assertThat(rs.next(), is(false));
	}

	@Test
	public void testFixUnsignedColumnBug() throws Exception {
		Connection c = context.getMaxwellConnection();
		this.savedSchema.save(c);

		c.createStatement().executeUpdate("update maxwell.schemas set version = 0 where id = " + this.savedSchema.getSchemaID());
		c.createStatement().executeUpdate("update maxwell.columns set is_signed = 1 where name = 'badcol'");

		MysqlSavedSchema restored = MysqlSavedSchema.restore(context, context.getInitialPosition());
		IntColumnDef cd = (IntColumnDef) restored.getSchema().findDatabase("shard_1").findTable("signed").findColumn("badcol");
		assertThat(cd.isSigned(), is(false));
	}
}
