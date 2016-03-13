package com.zendesk.maxwell.schema.ddl;

import java.util.UUID;
import java.io.IOException;

import com.zendesk.maxwell.RowInterface;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.producer.partitioners.PartitionKeyType;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DDLRow implements RowInterface {
	private final ResolvedSchemaChange change;
	private final Long timestamp;
	private final BinlogPosition nextPosition;

	public DDLRow(ResolvedSchemaChange change, Long timestamp, BinlogPosition nextPosition) {
		this.change = change;
		this.timestamp = timestamp;
		this.nextPosition = nextPosition;
	}

	public boolean isTXCommit() {
		return false;
	}

	public BinlogPosition getPosition() {
		return this.nextPosition;
	}

	public String rowKey() throws IOException {
		return UUID.randomUUID().toString();
	}

	public String getPartitionKey(PartitionKeyType keyType) {
		return UUID.randomUUID().toString();
	}

	public String toJSON() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(change);
	}
}

