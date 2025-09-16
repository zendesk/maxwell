package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolvedTableCreate extends ResolvedSchemaChange {

	static final Logger LOGGER = LoggerFactory.getLogger(
		ResolvedTableCreate.class
	);

	public String database;
	public String table;
	public Table def;

	public ResolvedTableCreate() {}

	public ResolvedTableCreate(Table t) {
		this.database = t.database;
		this.table = t.name;
		this.def = t;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		Database d = schema.findDatabaseOrThrow(this.database);

		if (d.hasTable(this.table)) {
			// Handle race condition: table already exists in schema (likely from initial capture)
			// while processing historical CREATE TABLE events from binlog.
			// This commonly occurs when Maxwell starts with empty state during concurrent migrations.
			Table existingTable = d.findTable(this.table);

			// Compare table definitions to detect if this is a benign race condition
			// vs a legitimate schema inconsistency
			if (
				existingTable != null &&
				existingTable.database.equals(this.def.database) &&
				existingTable.name.equals(this.def.name)
			) {
				LOGGER.debug(
					"Skipping CREATE TABLE for {}.{} - table already exists with compatible schema " +
						"(likely race condition during startup with concurrent migrations)",
					this.database,
					this.table
				);
				return;
			} else {
				throw new InvalidSchemaError(
					"Unexpectedly asked to create existing table " + this.table
				);
			}
		}

		d.addTable(this.def);
	}

	@Override
	public String databaseName() {
		return database;
	}

	@Override
	public String tableName() {
		return table;
	}
}
