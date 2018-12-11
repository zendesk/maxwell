package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.zendesk.maxwell.scripting.Scripting;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class JsonSerializer implements Serializable {

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonSerializer.class);

	private static final JsonFactory jsonFactory = new JsonFactory();

	private static final ThreadLocal<ByteArrayOutputStream> byteArrayThreadLocal =
			new ThreadLocal<ByteArrayOutputStream>() {
				@Override
				protected ByteArrayOutputStream initialValue() {
					return new ByteArrayOutputStream();
				}
			};

	private static final ThreadLocal<JsonGenerator> jsonGeneratorThreadLocal =
			new ThreadLocal<JsonGenerator>() {
				@Override
				protected JsonGenerator initialValue() {
					JsonGenerator g = null;
					try {
						g = jsonFactory.createGenerator(byteArrayThreadLocal.get());
					} catch (IOException e) {
						LOGGER.error("error initializing jsonGenerator", e);
						return null;
					}
					g.setRootValueSeparator(null);
					return g;
				}
			};

	public static final ThreadLocal<DataJsonGenerator> plaintextDataGeneratorThreadLocal =
			new ThreadLocal<DataJsonGenerator>() {
				@Override
				protected DataJsonGenerator initialValue() {
					return new PlaintextJsonGenerator(jsonGeneratorThreadLocal.get());
				}
			};

	public static final ThreadLocal<EncryptingJsonGenerator> encryptingJsonGeneratorThreadLocal =
			new ThreadLocal<EncryptingJsonGenerator>() {
				@Override
				protected EncryptingJsonGenerator initialValue() {
					try {
						return new EncryptingJsonGenerator(jsonGeneratorThreadLocal.get(), jsonFactory);
					} catch (IOException e) {
						LOGGER.error("error initializing EncryptingJsonGenerator", e);
						return null;
					}
				}
			};

	public static JsonGenerator resetJsonGenerator() {
		byteArrayThreadLocal.get().reset();
		return jsonGeneratorThreadLocal.get();
	}

	public void writeValueToJSON(JsonGenerator g, boolean includeNullField, String key, Object value) throws IOException {
		if (value == null && !includeNullField)
			return;

		if (value instanceof ScriptObjectMirror) {
			try {
				String json = Scripting.stringify((ScriptObjectMirror) value);
				writeValueToJSON(g, includeNullField, key, new RawJSONString(json));
			} catch (ScriptException e) {
				LOGGER.error("error stringifying json object:", e);
			}
		} else if (value instanceof List) { // sets come back from .asJSON as lists, and jackson can't deal with lists natively.
			List stringList = (List) value;

			g.writeArrayFieldStart(key);
			for (Object s : stringList) {
				g.writeObject(s);
			}
			g.writeEndArray();
		} else if (value instanceof RawJSONString) {
			// JSON column type, using binlog-connector's serializers.
			g.writeFieldName(key);
			g.writeRawValue(((RawJSONString) value).json);
		} else {
			g.writeObjectField(key, value);
		}
	}

	public String jsonFromStream() {
		ByteArrayOutputStream b = byteArrayThreadLocal.get();
		String s = b.toString();
		b.reset();
		return s;
	}
}
