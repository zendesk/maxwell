/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.zendesk.maxwell.replication.vitess;

import java.nio.charset.StandardCharsets;
import java.sql.Types;

/** Logical representation of both column type and value. */
public class ReplicationMessageColumn {
	private final String columnName;
	private final VitessType type;
	private final byte[] rawValue;

	public ReplicationMessageColumn(String columnName, VitessType type, byte[] rawValue) {
		this.columnName = columnName.intern();
		this.type = type;
		this.rawValue = rawValue;
	}

	public String getName() {
		return columnName;
	}

	public VitessType getType() {
		return type;
	}

	public Object getValue() {
		final VitessColumnValue value = new VitessColumnValue(rawValue);

		if (value.isNull()) {
			return null;
		}

		switch (type.getJdbcId()) {
			case Types.SMALLINT:
				return value.asShort();
			case Types.INTEGER:
				return value.asInteger();
			case Types.BIGINT:
				return value.asLong();
			case Types.BLOB:
			case Types.BINARY:
				return value.asBytes();
			case Types.VARCHAR:
				return value.asString();
			case Types.FLOAT:
				return value.asFloat();
			case Types.DOUBLE:
				return value.asDouble();
			default:
				break;
		}

		return value.asDefault(type);
	}

	public byte[] getRawValue() {
		return rawValue;
	}

	@Override
	public String toString() {
		return columnName + "=" + new String(rawValue, StandardCharsets.UTF_8);
	}
}
