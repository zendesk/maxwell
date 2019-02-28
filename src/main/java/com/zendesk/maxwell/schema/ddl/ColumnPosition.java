package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Table;

public class ColumnPosition {
	enum Position { FIRST, AFTER, DEFAULT };
	public static final int AFTER_NOT_FOUND = -999;

	public Position position;
	public String afterColumn;

	public ColumnPosition() {
		this.position = Position.DEFAULT;
	}

	public int index(Table t, Integer defaultIndex) {
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

			// see issue #1216
			if ( afterIdx == -1 )
				return AFTER_NOT_FOUND;

			return afterIdx + 1;
		}
		return -1;
	}
}
