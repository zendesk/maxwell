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
}
