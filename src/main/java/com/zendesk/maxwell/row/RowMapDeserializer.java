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
		ObjectNode node = jsonParser.getCodec().readTree(jsonParser);

		JsonNode encrypted = node.get("encrypted");
		if (encrypted != null) {
			String iv = encrypted.get("iv").textValue();
			String bytes = encrypted.get("bytes").textValue();

			String decryptedData;
			try {
				decryptedData = RowEncrypt.decrypt(bytes, this.secret_key, iv);
			} catch (Exception e) {
				throw new IOException(e);
			}
			JsonNode decrypted = mapper.readTree(decryptedData);
			if (!(decrypted instanceof ObjectNode)) {
				throw new ParseException("`encrypted` must be an object after decrypting.");
			}
			node.setAll((ObjectNode) decrypted);
		}

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
		JsonNode commit = node.get("commit");
		JsonNode data = node.get("data");
		JsonNode oldData = node.get("old");
		JsonNode comment = node.get("comment");

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

		if (commit != null && commit.asBoolean()) {
			rowMap.setTXCommit();
		}

		if (data == null){
			throw new ParseException("`data` is required and cannot be null.");
		}

		readDataInto(rowMap, data, false);

		if (oldData != null) {
			readDataInto(rowMap, oldData, true);
		}

		if (comment != null) {
			rowMap.setComment(comment.asText());
		}

		return rowMap;
	}

	private void readDataInto(RowMap dest, JsonNode data, boolean isOld) throws IOException {
		if (!(data instanceof ObjectNode)) {
			throw new ParseException("`" + (isOld ? "oldData" : "data") + "` cannot be parsed.");
		}

		Iterator keys = data.fieldNames();
		if (keys != null) {
			while (keys.hasNext()) {
				String key = (String) keys.next();
				JsonNode value = data.get(key);
				if (value.isValueNode()) {
					ValueNode valueNode = (ValueNode) value;
					if(isOld) {
						dest.putOldData(key, getValue(valueNode));
					} else {
						dest.putData(key, getValue(valueNode));
					}
				}
			}
		}
	}

	private Object getValue(ValueNode value)
	{
		if (value.isNull()) {
			return null;
		}

		if (value.numberType() != null) {
			switch (value.numberType()) {
				case LONG:
					return value.longValue();
				case DOUBLE:
					return value.doubleValue();
				case FLOAT:
					return value.floatValue();
				case INT:
					return value.intValue();
				case BIG_DECIMAL:
					return value.decimalValue();
				case BIG_INTEGER:
					return value.bigIntegerValue();
				default:
					return value.asText();
			}
		}

		if (value.isBoolean()) {
			return value.asBoolean();
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
