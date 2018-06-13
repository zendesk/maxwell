package com.zendesk.maxwell.core.schema.ddl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.core.config.MaxwellOutputConfig;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.config.BaseMaxwellOutputConfig;
import com.zendesk.maxwell.core.row.BaseRowMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

public class BaseDDLMap extends BaseRowMap implements DDLMap {
	private final ResolvedSchemaChange change;
	private final Long timestamp;
	private final String sql;
	private Position nextPosition;

	public BaseDDLMap(ResolvedSchemaChange change, Long timestamp, String sql, Position nextPosition) {
		super("ddl", change.databaseName(), change.tableName(), timestamp, new ArrayList<>(0), nextPosition);
		this.change = change;
		this.timestamp = timestamp;
		this.sql = sql;
		this.nextPosition = nextPosition;
	}

	@Override
	public String pkToJson(KeyFormat keyFormat) throws IOException {
		return UUID.randomUUID().toString();
	}

	@Override
	public boolean isTXCommit() {
		return false;
	}

	@Override
	public String toJSON() throws IOException {
		return toJSON(new BaseMaxwellOutputConfig());
	}

	@Override
	public String toJSON(MaxwellOutputConfig outputConfig) throws IOException {

		if(!outputConfig.isOutputDDL())
		return null;

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> changeMixin = mapper.convertValue(change, new TypeReference<Map<String, Object>>() { });
		changeMixin.put("ts", timestamp);
		changeMixin.put("sql", sql);
		BinlogPosition binlogPosition = nextPosition.getBinlogPosition();
		if (outputConfig.isIncludesBinlogPosition()) {
			changeMixin.put("position", binlogPosition.getFile() + ":" + binlogPosition.getOffset());
		}
		if (outputConfig.isIncludesGtidPosition()) {
			changeMixin.put("gtid", binlogPosition.getGtid());
		}
		return mapper.writeValueAsString(changeMixin);
	}

	@Override
	public boolean shouldOutput(MaxwellOutputConfig outputConfig) {
		return outputConfig.isOutputDDL();
	}

	@Override
	public String getSql() {
		return sql;
	}
}
