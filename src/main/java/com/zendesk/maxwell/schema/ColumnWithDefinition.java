package com.zendesk.maxwell.schema;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.google.code.or.common.glossary.column.AbstractDatetimeColumn;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

public class ColumnWithDefinition {
	public Column column;
	public ColumnDef definition;

	public ColumnWithDefinition(Column column, ColumnDef definition) {
		this.column = column;
		this.definition = definition;
	}

	private Object valueForJSON() {
		if (column instanceof AbstractDatetimeColumn) {
			return ((AbstractDatetimeColumn) column).getTimestampValue();
		}

		return column.getValue();
	}

	public Object asJSON() {
		Object value = valueForJSON();
		if ( value == null )
			return null;

		return definition.asJSON(value);
	}
}
