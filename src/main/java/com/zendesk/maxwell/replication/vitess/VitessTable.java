package com.zendesk.maxwell.replication.vitess;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

import binlogdata.Binlogdata.RowChange;
import io.vitess.proto.Query.Row;

public class VitessTable {
	private final String schemaName;
	private final String tableName;
	private final List<VitessColumn> columns;
	private final List<String> pkColumns;

	public VitessTable(String schemaName, String tableName, List<VitessColumn> columns, List<String> pkColumns) {
		this.schemaName = schemaName.intern();
		this.tableName = tableName.intern();
		this.columns = columns;
		this.pkColumns = pkColumns;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getQualifiedTableName() {
		return schemaName + "." + tableName;
	}

	public List<VitessColumn> getColumns() {
		return columns;
	}

	public List<String> getPkColumns() {
		return pkColumns;
	}

	public String toString() {
		return "Table [schemaName=" + schemaName
				+ ", tableName=" + tableName
				+ ", columns=" + columns
				+ ", pkColumns=" + pkColumns
				+ "]";
	}

	/**
	 * Resolve a specific row from vEvent data to a list of replication message
	 * columns (with values).
	 */
	public List<ReplicationMessageColumn> resolveColumnsFromRow(Row row) {
		int changedColumnsCnt = row.getLengthsCount();
		if (columns.size() != changedColumnsCnt) {
			throw new IllegalStateException(
					String.format(
							"The number of columns in the ROW event {} is different from the in-memory table schema {}.",
							row,
							this));
		}

		ByteString rawValues = row.getValues();
		int rawValueIndex = 0;
		List<ReplicationMessageColumn> eventColumns = new ArrayList<>(changedColumnsCnt);
		for (short i = 0; i < changedColumnsCnt; i++) {
			final VitessColumn columnDefinition = columns.get(i);
			final String columnName = columnDefinition.getName();
			final VitessType vitessType = columnDefinition.getType();

			final int rawValueLength = (int) row.getLengths(i);
			final byte[] rawValue = rawValueLength == -1
					? null
					: rawValues.substring(rawValueIndex, rawValueIndex + rawValueLength).toByteArray();
			if (rawValueLength != -1) {
				// no update to rawValueIndex when no value in the rawValue
				rawValueIndex += rawValueLength;
			}

			final ReplicationMessageColumn column = new ReplicationMessageColumn(columnName, vitessType, rawValue);
			eventColumns.add(column);
		}
		return eventColumns;
	}


}
