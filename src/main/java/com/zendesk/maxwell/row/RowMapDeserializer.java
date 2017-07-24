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
	private String secret_key;


	public RowMapDeserializer() {
		this(Class.class);
	}

	public RowMapDeserializer(String secret_key){
		this(null,secret_key);
	}


	public RowMapDeserializer(Class<?> vc) {
		super(vc);
	}

	public RowMapDeserializer(Class<?> vc, String secret_key){
		super(vc);
		this.secret_key = secret_key;
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
				ts.asLong() * 1000,
				new ArrayList<String>(),
				null
		);

		if (xid != null) {
			rowMap.setXid(xid.asLong());
		}

		if (data == null){
			throw new ParseException("`data` is required and cannot be null.");
		} else if (data instanceof ObjectNode) {
			rowMap = mapKeys(rowMap, data, false);
		} else if (data.isTextual()){
			String decryptedData = RowEncrypt.decrypt(data.textValue(), this.secret_key, node.get("init_vector").toString());
			JsonNode decryptedDataNode = mapper.readTree(decryptedData);
			if (decryptedDataNode instanceof ObjectNode) {
				rowMap = mapKeys(rowMap, decryptedDataNode, false);
			} else{
				throw new ParseException("`data` cannot be parsed.");
			}
		} else {
			throw new ParseException("`data` cannot be parsed.");
		}

		if (oldData != null) {
			if (oldData instanceof ObjectNode) {
				rowMap = mapKeys(rowMap, oldData, true);
			} else if (oldData.isTextual()) {
				String decryptedData = RowEncrypt
						.decrypt(oldData.textValue(), this.secret_key, node.get("init_vector").toString());
				JsonNode decryptedDataNode = mapper.readTree(decryptedData);
				if (decryptedDataNode instanceof ObjectNode) {
					rowMap = mapKeys(rowMap, decryptedDataNode, true);
				} else {
					throw new ParseException("`oldData` cannot be parsed.");
				}
			}
		}

		return rowMap;
	}

	private RowMap mapKeys(RowMap rowMap, JsonNode data, boolean isOld){
		Iterator keys = data.fieldNames();
		if (keys != null) {
			while (keys.hasNext()) {
				String key = (String) keys.next();
				JsonNode value = data.get(key);
				if (value.isValueNode()) {
					ValueNode valueNode = (ValueNode) value;
					if(isOld) {
						rowMap.putOldData(key, getValue(valueNode));
					} else {
						rowMap.putData(key, getValue(valueNode));
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

	private static ObjectMapper getMapper(String secret_key)
	{
		if (mapper == null) {
			mapper = new ObjectMapper();
			SimpleModule module = new SimpleModule();
			module.addDeserializer(RowMap.class, new RowMapDeserializer(secret_key));
			mapper.registerModule(module);
		}

		return mapper;
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

	public static RowMap createFromString(String json, String secret_key) throws IOException
	{
		return getMapper(secret_key).readValue(json, RowMap.class);
	}

	public static void resetMapper(){
		mapper = null;
	}
}
