package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.zendesk.maxwell.errors.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class RowMapDeserializer extends StdDeserializer<RowMap> {
	private static ObjectMapper mapper;

	public RowMapDeserializer() {
		this(null);
	}

	public RowMapDeserializer(Class<?> vc) {
		super(vc);
	}

	@Override
	public RowMap deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
		JsonNode node = jsonParser.getCodec().readTree(jsonParser);

		JsonNode type = node.get("type");
		if (type == null) {
			throw new ParseException("`type` is required and cannot be null.");
		}

		JsonNode database = node.get("database");
		if (database == null) {
			throw new ParseException("`database` is required and cannot be null.");
		}
		
		JsonNode table = node.get("table");
		if (table == null) {
			throw new ParseException("`table` is required and cannot be null.");
		}
		
		JsonNode ts = node.get("ts");
		if (ts == null) {
			throw new ParseException("`ts` is required and cannot be null.");
		}
		
		JsonNode xid = node.get("xid");
		JsonNode data = node.get("data");
		JsonNode oldData = node.get("old");

		RowMap rowMap = new RowMap(
				type.asText(),
				database.asText(),
				table.asText(),
				ts.asLong(),
				new ArrayList<String>(),
				null
		);

		if (xid != null) {
			rowMap.setXid(xid.asLong());
		}

		if (data instanceof ObjectNode) {
			Iterator keys = data.fieldNames();
			if (keys != null) {
				while (keys.hasNext()) {
					String key = (String) keys.next();
					JsonNode value = data.get(key);
					if (value.isValueNode()) {
						ValueNode valueNode = (ValueNode) value;
						rowMap.putData(key, getValue(valueNode));
					}
				}
			}
		} else {
			throw new ParseException("`data` is required and cannot be null.");
		}

		if (oldData instanceof ObjectNode) {
			Iterator keys = oldData.fieldNames();
			if (keys != null) {
				while (keys.hasNext()) {
					String key = (String) keys.next();
					JsonNode value = oldData.get(key);
					if (value.isValueNode()) {
						ValueNode valueNode = (ValueNode) value;
						rowMap.putOldData(key, getValue(valueNode));
					}
				}
			}
		}

		return rowMap;
	}

	private Object getValue(ValueNode value)
	{
		if (value.isNull()) {
			return null;
		}

		if (value.isBoolean()) {
			return value.asBoolean();
		}

		if (value.canConvertToInt()) {
			return value.asInt();
		}

		if (value.canConvertToLong()) {
			return value.asLong();
		}

		return value.asText();
	}

	private static ObjectMapper getMapper()
	{
		if (mapper == null) {
			mapper = new ObjectMapper();
			SimpleModule module = new SimpleModule();
			module.addDeserializer(RowMap.class, new RowMapDeserializer());
			mapper.registerModule(module);
		}

		return mapper;
	}

	public static RowMap createFromString(String json) throws IOException
	{
		return getMapper().readValue(json, RowMap.class);
	}
}
