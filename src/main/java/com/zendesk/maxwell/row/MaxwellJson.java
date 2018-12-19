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
import java.util.List;

public class MaxwellJson {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellJson.class);
	private static final JsonFactory jsonFactory = new JsonFactory();

	private final ByteArrayOutputStream buffer;
	private final JsonGenerator jsonGenerator;
	private final PlaintextJsonGenerator plaintextGenerator;
	private final EncryptingJsonGenerator encryptingGenerator;

	private static final ThreadLocal<MaxwellJson> instance = ThreadLocal.withInitial(() -> {
		try {
			return new MaxwellJson();
		} catch (IOException e) {
			LOGGER.error("error initializing MaxwellJson", e);
			return null;
		}
	});

	private MaxwellJson() throws IOException {
		buffer = new ByteArrayOutputStream();
		jsonGenerator = jsonFactory.createGenerator(buffer);
		jsonGenerator.setRootValueSeparator(null);
		plaintextGenerator = new PlaintextJsonGenerator(jsonGenerator);
		encryptingGenerator = new EncryptingJsonGenerator(jsonGenerator, jsonFactory);
	}

	public static MaxwellJson getInstance() {
		return instance.get();
	}

	public PlaintextJsonGenerator getPlaintextGenerator() {
		return plaintextGenerator;
	}

	public EncryptingJsonGenerator getEncryptingGenerator() {
		return encryptingGenerator;
	}

	public JsonGenerator reset() throws IOException {
		buffer.reset();
		return jsonGenerator;
	}

	public String consume() throws IOException {
		jsonGenerator.flush();
		String s = buffer.toString();
		buffer.reset();
		return s;
	}

	public static void writeValueToJSON(JsonGenerator g, boolean includeNullField, String key, Object value) throws IOException {
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
}
