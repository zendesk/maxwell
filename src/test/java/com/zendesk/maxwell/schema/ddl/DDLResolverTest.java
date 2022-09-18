package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.zendesk.maxwell.MaxwellTestWithIsolatedServer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;

import com.zendesk.maxwell.schema.columndef.StringColumnDef;

public class DDLResolverTest extends MaxwellTestWithIsolatedServer {
	@BeforeClass
	public static void setUp() throws Exception {
		server.executeQuery("create database already_there");
	}

	private <T extends SchemaChange> T parse(String sql, String database, Class<T> type) {
		List<SchemaChange> changeList = SchemaChange.parse(database, sql);
		return type.cast(changeList.get(0));
	}

	private Schema getSchema() throws Exception {
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection(), buildContext().getCaseSensitivity());
		return capturer.capture();
	}

	@Test
	public void testDatabaseCreateResolveCharset() throws Exception {
		server.executeQuery("set global character_set_server = 'latin1'");
		server.resetConnection();

		Schema topSchema = getSchema();
		assertEquals("latin1", topSchema.getCharset());

		DatabaseCreate c = parse("CREATE DATABASE `foofoo`", null, DatabaseCreate.class);
		assertThat(c.charset, is(nullValue()));

		ResolvedDatabaseCreate resolved = c.resolve(topSchema);
		assertThat(resolved.charset, is("latin1"));
	}

	@Test
	public void testDatabaseCreateResolveIfNotExists2() throws Exception {
		Schema schema = getSchema();

		DatabaseCreate c = parse("CREATE DATABASE if not exists already_there", null, DatabaseCreate.class);
		assertThat(c.resolve(schema), is(nullValue()));

		ResolvedDatabaseCreate resolved = parse("CREATE DATABASE if not exists not_there", null, DatabaseCreate.class).resolve(schema);
	}

	@Test
	public void testDatabaseDropResolveIfExists2() throws Exception {
		DatabaseDrop d = parse("DROP DATABASE if exists not_totally_there", null, DatabaseDrop.class);
		assertThat(d.resolve(getSchema()), is(nullValue()));
	}

	@Test
	public void testCreateTableResolveIfNotExists() throws Exception {
		server.executeQuery("create table test.table_already_there ( id int )");
		TableCreate c = parse("CREATE TABLE if not exists `table_already_there` ( id int(10) )", "test", TableCreate.class);
		assertThat(c.resolve(getSchema()), is(nullValue()));
	}

	@Test
	public void testCreateTableResolveCharset() throws Exception {
		server.executeQuery("create database test_enc character set 'latin2'");

		TableCreate c = parse("CREATE TABLE `test_enc`.`te` ( c varchar, d varchar character set 'utf8' )", "test_enc", TableCreate.class);
		ResolvedTableCreate rc = c.resolve(getSchema());

		assertThat(rc.def.charset, is("latin2"));
		assertThat(((StringColumnDef) rc.def.getColumnList().get(0)).getCharset(), is("latin2"));
		assertThat(((StringColumnDef) rc.def.getColumnList().get(1)).getCharset(), is("utf8"));

	}

	@Test
	public void testCreateTableResolveLike() throws Exception {
		server.executeQuery("alter database `test` character set 'utf8'");
		server.executeQuery("create table `test`.`test_alike` ( ii int, aa char, PRIMARY KEY (ii))");
		TableCreate c = parse("CREATE TABLE alike_2 like `test`.`test_alike`", "test", TableCreate.class);
		ResolvedTableCreate rc = c.resolve(getSchema());
		assertThat(rc.def.getColumnList().size(), is(2));
		assertThat(rc.def.getPKList().get(0), is("ii"));
		String charset = ((StringColumnDef) rc.def.getColumnList().get(1)).getCharset();
		assertTrue(charset.startsWith("utf8"));
	}

	@Test
	public void testDropTableResolve() throws Exception {
		TableDrop t = parse("drop table if exists `flkj`.`lakjsdflaj`", null, TableDrop.class);
		assertThat(t.resolve(getSchema()), is(nullValue()));
	}
}
