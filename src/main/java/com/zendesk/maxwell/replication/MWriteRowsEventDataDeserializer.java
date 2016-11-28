package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by ben on 11/27/16.
 */
public class MWriteRowsEventDataDeserializer extends WriteRowsEventDataDeserializer {
	public MWriteRowsEventDataDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
		super(tableMapEventByTableId);
	}

	@Override
	protected Serializable deserializeString(int length, ByteArrayInputStream inputStream) throws IOException {
		int stringLength = length < 256 ? inputStream.readInteger(1) : inputStream.readInteger(2);
		return inputStream.read(stringLength);
	}

	@Override
	protected Serializable deserializeVarString(int meta, ByteArrayInputStream inputStream) throws IOException {
		return deserializeString(meta, inputStream);
	}
}
