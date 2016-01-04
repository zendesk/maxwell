package com.zendesk.maxwell;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

public class ColumnWithDefinition {
	public Column column;
	public ColumnDef definition;

	public ColumnWithDefinition(Column column, ColumnDef definition) {
		this.column = column;
		this.definition = definition;
	}

	private Object valueForJSON() {
		if (column instanceof DatetimeColumn)
			return ((DatetimeColumn) column).getLongValue();
		return column.getValue();
	}

	public Object asJSON() {
		Object value = valueForJSON();
		if ( value == null )
			return null;

		return definition.asJSON(value);
	}
}

