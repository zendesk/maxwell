package com.zendesk.maxwell.core.row;

import com.zendesk.maxwell.core.replication.Position;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BaseRowMapFactory implements RowMapFactory {
	@Override
	public RowMap createFor(String type, String database, String table, Long timestampMillis, List<String> pkColumns, Position nextPosition, String rowQuery) {
		return new BaseRowMap(type, database, table, timestampMillis, pkColumns, nextPosition, rowQuery);
	}

	@Override
	public RowMap createFor(String type, String database, String table, Long timestampMillis, List<String> pkColumns, Position nextPosition) {
		return new BaseRowMap(type, database, table, timestampMillis, pkColumns, nextPosition, null);
	}
}
