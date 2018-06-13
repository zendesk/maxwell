package com.zendesk.maxwell.core.row;

import com.zendesk.maxwell.core.replication.Position;

import java.util.List;

public interface RowMapFactory {
	RowMap createFor(String type, String database, String table, Long timestampMillis, List<String> pkColumns, Position nextPosition, String rowQuery);

	RowMap createFor(String type, String database, String table, Long timestampMillis, List<String> pkColumns, Position nextPosition);
}
