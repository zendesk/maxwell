package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.zendesk.maxwell.AbstractMaxwellTest;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;

import com.zendesk.maxwell.schema.columndef.StringColumnDef;

public class DDLResolverTest extends AbstractMaxwellTest {
	@BeforeClass
	public static void setUp() throws Exception {
		server.executeQuery("create database already_there");
	}

	@Override
	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}


	private Schema getSchema() throws Exception {
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection(), buildContext().getCaseSensitivity());
		return capturer.capture();
	}

	private DatabaseCreate parseDatabaseCreate(String sql) {
		List<SchemaChange> changeList = SchemaChange.parse(null, sql);
		return (DatabaseCreate) changeList.get(0);
	}

	@Test
	public void testDatabaseCreateResolveCharset() throws Exception {
		server.executeQuery("set global character_set_server = 'latin1'");
		server.resetConnection();

		Schema topSchema = getSchema();
		assertEquals("latin1", topSchema.getCharset());

		DatabaseCreate c = parseDatabaseCreate("CREATE DATABASE `foofoo`");
		assertThat(c.charset, is(nullValue()));

		c = c.resolve(topSchema);
		assertThat(c.charset, is("latin1"));
	}

	@Test(expected = SchemaSyncError.class)
	public void testDatabaseCreateResolveIfNotExists1() throws Exception {
		Schema schema = getSchema();

		DatabaseCreate c = parseDatabaseCreate("CREATE DATABASE already_there");
		c.resolve(schema);
	}

	@Test
	public void testDatabaseCreateResolveIfNotExists2() throws Exception {
		Schema schema = getSchema();

		DatabaseCreate c = parseDatabaseCreate("CREATE DATABASE if not exists already_there");
		assertThat(c.resolve(schema), is(nullValue()));

		c = parseDatabaseCreate("CREATE DATABASE if not exists not_there").resolve(schema);
	}

	private DatabaseDrop parseDatabaseDrop(String sql) {
		List<SchemaChange> changeList = SchemaChange.parse(null, sql);
		return (DatabaseDrop) changeList.get(0);
	}

	@Test(expected = SchemaSyncError.class)
	public void testDatabaseDropResolveIfExists1() throws Exception {
		DatabaseDrop d = parseDatabaseDrop("DROP DATABASE not_totally_there");
		d.resolve(getSchema());
	}

	@Test
	public void testDatabaseDropResolveIfExists2() throws Exception {
		DatabaseDrop d = parseDatabaseDrop("DROP DATABASE if exists not_totally_there");
		assertThat(d.resolve(getSchema()), is(nullValue()));
	}

	private TableCreate parseTableCreate(String sql, String database) {
		List<SchemaChange> changeList = SchemaChange.parse(database, sql);
		return (TableCreate) changeList.get(0);
	}

	@Test
	public void testCreateTableResolveIfNotExists() throws Exception {
		server.executeQuery("create table test.table_already_there ( id int )");
		TableCreate c = parseTableCreate("CREATE TABLE if not exists `table_already_there` ( id int(10) )", "test");
		assertThat(c.resolve(getSchema()), is(nullValue()));
	}

	@Test(expected = SchemaSyncError.class)
	public void testCreateTableResolveThrowsError() throws Exception {
		server.executeQuery("create table test.table_already_there_2 ( id int )");
		TableCreate c = parseTableCreate("CREATE TABLE `table_already_there_2` ( id int(10) )", "test");
		c.resolve(getSchema());
	}

	@Test
	public void testCreateTableResolveCharset() throws Exception {
		server.executeQuery("create database test_enc character set 'latin2'");

		TableCreate c = parseTableCreate("CREATE TABLE `test_enc`.`te` ( c varchar, d varchar character set 'utf8' )", "test_enc");
		c = c.resolve(getSchema());

		assertThat(c.charset, is("latin2"));
		assertThat(((StringColumnDef) c.columns.get(0)).charset, is("latin2"));
		assertThat(((StringColumnDef) c.columns.get(1)).charset, is("utf8"));

	}

	@Test
	public void testCreateTableResolveLike() throws Exception {
		server.executeQuery("alter database `test` character set 'utf8'");
		server.executeQuery("create table `test`.`test_alike` ( ii int, aa char, PRIMARY KEY (ii))");
		TableCreate c = parseTableCreate("CREATE TABLE alike_2 like `test`.`test_alike`", "test");

		c = c.resolve(getSchema());
		assertThat(c.columns.size(), is(2));
		assertThat(c.pks.get(0), is("ii"));
		assertThat(((StringColumnDef) c.columns.get(1)).charset, is("utf8"));
	}


}
