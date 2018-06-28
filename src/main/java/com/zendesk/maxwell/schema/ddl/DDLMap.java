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
	private Position position;

	public DDLMap(ResolvedSchemaChange change, Long timestamp, String sql, Position position, Position nextPosition) {
		super("ddl", change.databaseName(), change.tableName(), timestamp, new ArrayList<>(0), position, nextPosition, sql);
		this.change = change;
		this.timestamp = timestamp;
		this.sql = sql;
		this.position = position;
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

	public Map<String, Object> getChangeMap() {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.convertValue(change, new TypeReference<Map<String, Object>>() { });
	}

	@Override
	public String toJSON(MaxwellOutputConfig outputConfig) throws IOException {
		if(!outputConfig.outputDDL)
			return null;

		Map<String, Object> map = getChangeMap();
		map.put("ts", timestamp);
		map.put("sql", sql);

		map.putAll(getExtraAttributes());

		BinlogPosition binlogPosition = position.getBinlogPosition();
		if ( outputConfig.includesBinlogPosition ) {
			map.put("position", binlogPosition.getFile() + ":" + binlogPosition.getOffset());
		}
		if ( outputConfig.includesGtidPosition) {
			map.put("gtid", binlogPosition.getGtid());
		}
		return new ObjectMapper().writeValueAsString(map);
	}

	@Override
	public boolean shouldOutput(MaxwellOutputConfig outputConfig) {
		return outputConfig.outputDDL && !this.suppressed;
	}

	public String getSql() {
		return sql;
	}
}
