package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.core.row.RowMap;

public interface DDLMap extends RowMap {
	String getSql();
}
