package com.zendesk.maxwell.api.schema.ddl;

import com.zendesk.maxwell.api.config.MaxwellOutputConfig;
import com.zendesk.maxwell.api.row.RowMap;

import java.io.IOException;

public interface DDLMap extends RowMap {
	String getSql();
}
