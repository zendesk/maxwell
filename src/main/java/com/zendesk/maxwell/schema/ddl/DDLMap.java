package com.zendesk.maxwell.schema.ddl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

public class DDLMap extends RowMap {
	private final ResolvedSchemaChange change;
	private final Long timestamp;
	private final String sql;
	private Position nextPosition;

	public DDLMap(ResolvedSchemaChange change, Long timestamp, String sql, Position nextPosition) {
		super("ddl", change.databaseName(), change.tableName(), timestamp, new ArrayList<>(0), nextPosition);
		this.change = change;
		this.timestamp = timestamp;
		this.sql = sql;
		this.nextPosition = nextPosition;
	}

	public String pkToJson(KeyFormat keyFormat) throws IOException {
		return UUID.randomUUID().toString();
	}

	public boolean isTXCommit() {
		return false;
	}

	public String toJSON() throws IOException {
		return toJSON(new MaxwellOutputConfig());
	}

	@Override
	public String toJSON(MaxwellOutputConfig outputConfig) throws IOException {

		if(!outputConfig.outputDDL)
		return null;

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> changeMixin = mapper.convertValue(change, new TypeReference<Map<String, Object>>() { });
		changeMixin.put("ts", timestamp);
		changeMixin.put("sql", sql);
		BinlogPosition binlogPosition = nextPosition.getBinlogPosition();
		if ( outputConfig.includesBinlogPosition ) {
			changeMixin.put("position", binlogPosition.getFile() + ":" + binlogPosition.getOffset());
		}
		if ( outputConfig.includesGtidPosition) {
			changeMixin.put("gtid", binlogPosition.getGtid());
		}
		return mapper.writeValueAsString(changeMixin);
	}

	@Override
	public boolean shouldOutput(MaxwellOutputConfig outputConfig) {
		return outputConfig.outputDDL;
	}

	public String getSql() {
		return sql;
	}
}
