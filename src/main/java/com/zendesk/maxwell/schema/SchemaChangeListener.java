package com.zendesk.maxwell.schema;

import java.util.List;

import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

public interface SchemaChangeListener {
	
	void onSchemaChange(List<ResolvedSchemaChange> changes, long newSchemaId, Schema newSchema);
	
}
