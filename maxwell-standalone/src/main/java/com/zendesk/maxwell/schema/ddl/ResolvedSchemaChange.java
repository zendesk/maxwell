package com.zendesk.maxwell.schema.ddl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Schema;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="type")
@JsonSubTypes({
		@JsonSubTypes.Type(value = ResolvedTableAlter.class, name = "table-alter"),
		@JsonSubTypes.Type(value = ResolvedTableCreate.class, name = "table-create"),
		@JsonSubTypes.Type(value = ResolvedTableDrop.class, name = "table-drop"),
		@JsonSubTypes.Type(value = ResolvedDatabaseAlter.class, name = "database-alter"),
		@JsonSubTypes.Type(value = ResolvedDatabaseCreate.class, name = "database-create"),
		@JsonSubTypes.Type(value = ResolvedDatabaseDrop.class, name = "database-drop"),
})

public abstract class ResolvedSchemaChange {
	public abstract void apply(Schema originalSchema) throws InvalidSchemaError;

	public abstract String databaseName();

	public abstract String tableName();

	public boolean shouldOutput(MaxwellFilter filter) {
		return MaxwellFilter.matches(filter, databaseName(), tableName());
	};
}
