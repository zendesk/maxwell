package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class ResolvedTableCreateRaceConditionTest {

	private Schema schema;
	private Database database;

	@Before
	public void setUp() {
		schema = new Schema(
			new ArrayList<>(),
			"utf8",
			CaseSensitivity.CASE_SENSITIVE
		);
		database = new Database("test_db", "utf8");
		schema.addDatabase(database);
	}

	@Test
	public void testCreateTableWhenTableAlreadyExists_ShouldNotThrowException()
		throws Exception {
		// Simulate the race condition scenario:
		// 1. Maxwell captures schema with existing table
		List<ColumnDef> columns = new ArrayList<>();
		columns.add(StringColumnDef.create("id", "varchar", (short) 0, "utf8"));
		Table existingTable = new Table(
			"test_db",
			"race_table",
			"utf8",
			columns,
			Arrays.asList("id")
		);
		database.addTable(existingTable);

		// 2. Maxwell processes CREATE TABLE from binlog for the same table
		List<ColumnDef> incomingColumns = new ArrayList<>();
		incomingColumns.add(
			StringColumnDef.create("id", "varchar", (short) 0, "utf8")
		);
		Table incomingTable = new Table(
			"test_db",
			"race_table",
			"utf8",
			incomingColumns,
			Arrays.asList("id")
		);
		ResolvedTableCreate resolvedCreate = new ResolvedTableCreate(
			incomingTable
		);

		// 3. Should not throw "Unexpectedly asked to create existing table" error
		resolvedCreate.apply(schema);

		// 4. Table should still exist (unchanged)
		Table resultTable = database.findTable("race_table");
		assertThat(resultTable, is(existingTable));
	}

	@Test
	public void testCreateTableWhenTableDoesNotExist_ShouldCreateTable()
		throws Exception {
		// Normal case: table doesn't exist, should be created
		List<ColumnDef> columns = new ArrayList<>();
		columns.add(StringColumnDef.create("id", "varchar", (short) 0, "utf8"));
		Table newTable = new Table(
			"test_db",
			"new_table",
			"utf8",
			columns,
			Arrays.asList("id")
		);
		ResolvedTableCreate resolvedCreate = new ResolvedTableCreate(newTable);

		// Should not have the table initially
		assertThat(database.findTable("new_table"), is(nullValue()));

		// Apply the create
		resolvedCreate.apply(schema);

		// Should now have the table
		Table resultTable = database.findTable("new_table");
		assertThat(resultTable.name, is("new_table"));
		assertThat(resultTable.database, is("test_db"));
	}

	@Test
	public void testCreateTableWithIncompatibleSchema_ShouldNotThrowButLogWarning()
		throws Exception {
		// Simulate case where existing table has different structure
		List<ColumnDef> existingColumns = new ArrayList<>();
		existingColumns.add(
			StringColumnDef.create("id", "varchar", (short) 0, "utf8")
		);
		Table existingTable = new Table(
			"test_db",
			"conflict_table",
			"utf8",
			existingColumns,
			Arrays.asList("id")
		);
		database.addTable(existingTable);

		// Incoming table has different column name (simulating schema drift)
		List<ColumnDef> incomingColumns = new ArrayList<>();
		incomingColumns.add(
			StringColumnDef.create("user_id", "varchar", (short) 0, "utf8")
		);
		Table incomingTable = new Table(
			"test_db",
			"conflict_table",
			"utf8",
			incomingColumns,
			Arrays.asList("user_id")
		);
		ResolvedTableCreate resolvedCreate = new ResolvedTableCreate(
			incomingTable
		);

		// Should not throw exception (to prevent crashes during startup)
		resolvedCreate.apply(schema);

		// Original table should remain unchanged
		Table resultTable = database.findTable("conflict_table");
		assertThat(resultTable, is(existingTable));
		assertThat(resultTable.findColumn("id").getName(), is("id"));
	}

	@Test
	public void testDatabaseName() throws Exception {
		Table table = new Table(
			"test_db",
			"some_table",
			"utf8",
			new ArrayList<>(),
			new ArrayList<>()
		);
		ResolvedTableCreate resolvedCreate = new ResolvedTableCreate(table);
		assertThat(resolvedCreate.databaseName(), is("test_db"));
	}

	@Test
	public void testTableName() throws Exception {
		Table table = new Table(
			"test_db",
			"some_table",
			"utf8",
			new ArrayList<>(),
			new ArrayList<>()
		);
		ResolvedTableCreate resolvedCreate = new ResolvedTableCreate(table);
		assertThat(resolvedCreate.tableName(), is("some_table"));
	}
}
