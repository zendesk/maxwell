package com.zendesk.maxwell.replication.vitess;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import binlogdata.Binlogdata.FieldEvent;
import io.vitess.proto.Query.Field;

// An in-memory representation of a Vitess schema.
public class VitessSchema {
	private static final Logger LOGGER = LoggerFactory.getLogger(VitessSchema.class);

	// See all flags:
	// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/group__group__cs__column__definition__flags.html
	private static final int NOT_NULL_FLAG = 1;
	private static final int PRI_KEY_FLAG = 1 << 1;
	private static final int UNIQUE_KEY_FLAG = 1 << 2;

	private final Map<String, VitessTable> tables = new HashMap<>();

	public void addTable(VitessTable table) {
		String tableName = table.getQualifiedTableName().intern();
		if (tables.containsKey(tableName)) {
			LOGGER.info("Schema change detected for: {}", table);
		} else {
			LOGGER.info("Table schema received for: {}", table);
		}
		tables.put(tableName, table);
	}

	public VitessTable getTable(String name) {
		return tables.get(name);
	}

	public void processFieldEvent(FieldEvent event) {
		if (event == null) {
			throw new RuntimeException(String.format("fieldEvent is expected from {}", event));
		}

		String qualifiedTableName = event.getTableName();
		String[] schemaTableTuple = qualifiedTableName.split("\\.");
		if (schemaTableTuple.length != 2) {
			throw new RuntimeException(
					String.format(
							"Handling FIELD VEvent. schemaTableTuple should have schema name and table name but has size {}. {} is skipped",
							schemaTableTuple.length,
							event));
		}

		LOGGER.debug("Handling FIELD VEvent: {}", event);
		String schemaName = schemaTableTuple[0];
		String tableName = schemaTableTuple[1];
		int columnCount = event.getFieldsCount();

		List<ColumnMetaData> columns = new ArrayList<>(columnCount);
		for (short i = 0; i < columnCount; ++i) {
			Field field = event.getFields(i);
			String columnName = validateColumnName(field.getName(), schemaName, tableName);

			VitessType vitessType = VitessType.resolve(field);
			if (vitessType.getJdbcId() == Types.OTHER) {
				LOGGER.error("Cannot resolve JDBC type from VStream field {}", field);
			}

			KeyMetaData keyMetaData = KeyMetaData.NONE;
			if ((field.getFlags() & PRI_KEY_FLAG) != 0) {
				keyMetaData = KeyMetaData.IS_KEY;
			} else if ((field.getFlags() & UNIQUE_KEY_FLAG) != 0) {
				keyMetaData = KeyMetaData.IS_UNIQUE_KEY;
			}
			boolean optional = (field.getFlags() & NOT_NULL_FLAG) == 0;

			columns.add(new ColumnMetaData(columnName, vitessType, optional, keyMetaData));
		}

		VitessTable table = createTable(schemaName, tableName, columns);
		addTable(table);
	}

	private static String validateColumnName(String columnName, String schemaName, String tableName) {
		int length = columnName.length();
		if (length == 0) {
			throw new IllegalArgumentException(
					String.format("Empty column name from schema: %s, table: %s", schemaName, tableName));
		}

		// Vitess VStreamer schema reloading transient bug could cause column names to
		// be anonymized to @1, @2, etc
		// We want to fail in this case instead of sending the corrupted row events with
		// @1, @2 as column names.
		char first = columnName.charAt(0);
		if (first == '@') {
			throw new IllegalArgumentException(
					String.format(
							"Illegal prefix '@' for column: %s, from schema: %s, table: %s",
							columnName,
							schemaName,
							tableName));
		}

		return columnName;
	}

	private VitessTable createTable(String schemaName, String tableName, List<ColumnMetaData> columnsMetaData) {
		List<VitessColumn> columns = new ArrayList<>(columnsMetaData.size());
		List<String> pkColumns = new ArrayList<>();

		for (ColumnMetaData columnMetaData : columnsMetaData) {
			VitessColumn column = new VitessColumn(columnMetaData.getColumnName(), columnMetaData.getVitessType());
			columns.add(column);

			if (columnMetaData.getKeyMetaData() == KeyMetaData.IS_KEY) {
				pkColumns.add(columnMetaData.getColumnName());
			}
		}

		return new VitessTable(schemaName, tableName, columns, pkColumns);
	}
}
