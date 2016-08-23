package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Table;

public class ColumnPosition {
	enum Position { FIRST, AFTER, DEFAULT };

	public Position position;
	public String afterColumn;

	public ColumnPosition() {
		this.position = Position.DEFAULT;
	}

	public int index(Table t, Integer defaultIndex) throws InvalidSchemaError {
		switch(position) {
		case FIRST:
			return 0;
		case DEFAULT:
			if ( defaultIndex != null )
				return defaultIndex;
			else
				return t.getColumnList().size();

		case AFTER:
			int afterIdx = t.findColumnIndex(afterColumn);
			if ( afterIdx == -1 )
				throw new InvalidSchemaError("Could not find column " + afterColumn + " (needed in AFTER statement)");
			return afterIdx + 1;
		}
		return -1;
	}
}
