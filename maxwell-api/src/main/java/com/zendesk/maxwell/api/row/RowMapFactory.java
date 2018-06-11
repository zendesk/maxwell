package com.zendesk.maxwell.api.row;

import com.zendesk.maxwell.api.replication.Position;

import java.util.List;

public interface RowMapFactory {
	RowMap createFor(String type, String database, String table, Long timestampMillis, List<String> pkColumns, Position nextPosition, String rowQuery);

	RowMap createFor(String type, String database, String table, Long timestampMillis, List<String> pkColumns, Position nextPosition);
}
