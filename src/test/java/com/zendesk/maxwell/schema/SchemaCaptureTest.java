package com.zendesk.maxwell.schema;

import static com.zendesk.maxwell.MaxwellTestSupport.getSQLDir;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellTestWithIsolatedServer;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.*;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public class SchemaCaptureTest extends MaxwellTestWithIsolatedServer {
	private SchemaCapturer capturer;

	@Before
	public void setUp() throws Exception {
		server.getConnection().createStatement().executeUpdate("CREATE DATABASE if not exists test");
		this.capturer = new SchemaCapturer(server.getConnection(), CaseSensitivity.CASE_SENSITIVE);
	}

	@Test
	public void testDatabases() throws SQLException, InvalidSchemaError {
		Schema s = capturer.capture();
		String dbs = StringUtils.join(s.getDatabaseNames().iterator(), ":");

		if ( server.getVersion().atLeast(server.VERSION_5_7) )
			assertEquals("maxwell:mysql:shard_1:shard_2:sys:test", dbs);
		else
			assertEquals("maxwell:mysql:shard_1:shard_2:test", dbs);
	}

	@Test
	public void testOneDatabase() throws SQLException, InvalidSchemaError {
		SchemaCapturer sc = new SchemaCapturer(server.getConnection(), CaseSensitivity.CASE_SENSITIVE, "shard_1");
		Schema s = sc.capture();

		String dbs = StringUtils.join(s.getDatabaseNames().iterator(), ":");
		assertEquals("shard_1", dbs);
	}

	@Test
	public void testTables() throws SQLException, InvalidSchemaError {
		Schema s = capturer.capture();

		Database shard1DB = s.findDatabase("shard_1");
		assert(shard1DB != null);

		List<String> nameList = shard1DB.getTableNames();
		nameList.sort(String::compareTo);

		assertEquals("ints:mediumints:minimal:sharded", StringUtils.join(nameList.iterator(), ":"));
	}

	@Test
	public void testTablefilter() throws Exception {
		SchemaCapturer sc =
			new SchemaCapturer(server.getConnection(), CaseSensitivity.CASE_SENSITIVE, "shard_1", "ints");
		Schema schema = sc.capture();
		assertEquals(1, schema.getDatabases().iterator().next().getTableList().size());
	}

	@Test
	public void testColumns() throws SQLException, InvalidSchemaError {
		Schema s = capturer.capture();

		Table sharded = s.findDatabase("shard_1").findTable("sharded");
		assert(sharded != null);

		ColumnDef columns[];

		columns = sharded.getColumnList().toArray(new ColumnDef[0]);

		assertThat(columns[0], notNullValue());
		assertThat(columns[0], instanceOf(BigIntColumnDef.class));
		assertThat(columns[0].getName(), is("id"));
		assertEquals(0, columns[0].getPos());

		assertThat(columns[1], allOf(notNullValue(), instanceOf(IntColumnDef.class)));
		assertThat(columns[1].getName(), is("account_id"));
		assertThat(columns[1], instanceOf(IntColumnDef.class));
		assertThat(((IntColumnDef) columns[1]).isSigned(), is(false));

		if ( server.getVersion().atLeast(server.VERSION_5_6) ) {
			assertThat(columns[10].getName(), is("timestamp2_field"));
			assertThat(columns[10], instanceOf(DateTimeColumnDef.class));
			assertThat(((DateTimeColumnDef) columns[10]).getColumnLength(), is(3L));

			assertThat(columns[11].getName(), is("datetime2_field"));
			assertThat(columns[11], instanceOf(DateTimeColumnDef.class));
			assertThat(((DateTimeColumnDef) columns[11]).getColumnLength(), is(6L));

			assertThat(columns[12].getName(), is("time2_field"));
			assertThat(columns[12], instanceOf(TimeColumnDef.class));
			assertThat(((TimeColumnDef) columns[12]).getColumnLength(), is(6L));
		}
	}

	@Test
	public void testPKs() throws SQLException, InvalidSchemaError {
		Schema s = capturer.capture();

		Table sharded = s.findDatabase("shard_1").findTable("sharded");
		List<String> pk = sharded.getPKList();
		assertThat(pk, notNullValue());
		assertThat(pk.size(), is(2));
		assertThat(pk.get(0), is("id"));
		assertThat(pk.get(1), is("account_id"));
	}

	@Test
	public void testEnums() throws SQLException, InvalidSchemaError, IOException {
		byte[] sql = Files.readAllBytes(Paths.get(getSQLDir() + "/schema/enum.sql"));
		server.executeList(Collections.singletonList(new String(sql)));

		Schema s = capturer.capture();

		Table enumTest = s.findDatabase("test").findTable("enum_test");
		assert(enumTest != null);

		ColumnDef[] columns = enumTest.getColumnList().toArray(new ColumnDef[0]);

		assertThat(columns[0], notNullValue());
		assertThat(columns[0], instanceOf(EnumColumnDef.class));
		assertThat(columns[0].getName(), is("language"));
		assertEquals(((EnumColumnDef) columns[0]).getEnumValues(), List.of("en-US", "de-DE"));

		assertThat(columns[1], notNullValue());
		assertThat(columns[1], instanceOf(EnumColumnDef.class));
		assertThat(columns[1].getName(), is("decimal_separator"));
		assertEquals(((EnumColumnDef) columns[1]).getEnumValues(), List.of(",", "."));
	}
	
	@Test
	public void testExtractEnumValues() throws Exception {
		String expandedType = "enum('a')";
		String[] result = SchemaCapturer.extractEnumValues(expandedType);
		assertEquals(1, result.length);
		assertEquals("a", result[0]);

		expandedType = "enum('a','b','c','d')";
		result = SchemaCapturer.extractEnumValues(expandedType);
		assertEquals(4, result.length);
		assertEquals("a", result[0]);
		assertEquals("b", result[1]);
		assertEquals("c", result[2]);
		assertEquals("d", result[3]);

		expandedType = "enum('','b','c','d')";
		result = SchemaCapturer.extractEnumValues(expandedType);
		assertEquals(4, result.length);
		assertEquals("", result[0]);
		assertEquals("b", result[1]);
		assertEquals("c", result[2]);
		assertEquals("d", result[3]);
		
		expandedType = "enum('a','b\'b','c')";
		result = SchemaCapturer.extractEnumValues(expandedType);
		assertEquals(3, result.length);
		assertEquals("a", result[0]);
		assertEquals("b'b", result[1]);
		assertEquals("c", result[2]);
		
		expandedType = "enum('','.',',','\\','\\'','\\,',','','b')";
		result = SchemaCapturer.extractEnumValues(expandedType);
		assertEquals(8, result.length);
		assertEquals("", result[0]);
		assertEquals(".", result[1]);
		assertEquals(",", result[2]);
		assertEquals("\\", result[3]);
		assertEquals("\\'", result[4]);
		assertEquals("\\,", result[5]);
		assertEquals(",'", result[6]);
		assertEquals("b", result[7]);
	}

	private long getUsedMem() {
		Runtime r = Runtime.getRuntime();
		return r.totalMemory() - r.freeMemory();
	}

	@Ignore
	@Test
	public void testHugeCaptureMemUsage() throws Exception {
		Runtime.getRuntime().gc();
		System.out.println("usage before: " + getUsedMem());
		generateHugeSchema();
		Schema s = capturer.capture();
		Runtime.getRuntime().gc();
		Runtime.getRuntime().gc();
		System.out.println("usage after: " + getUsedMem());
		System.out.println(s.getCharset());
	}
}
