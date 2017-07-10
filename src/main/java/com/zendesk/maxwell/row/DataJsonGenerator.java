package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

interface DataJsonGenerator {
	// A wrapper around JsonGenerator for the `data` (and `old`) payload in a RowMap,
	// in order to support encryption.

	// The plaintext implementation just forwards writes, while the
	// encrypted implementation writes to a local buffer and
	// writes an encrypted contents on end

	// outer object
	void begin(String key) throws IOException;
	void end() throws IOException;

	// contents
	void writeArrayFieldStart(String key) throws IOException;
	void writeObject(Object s) throws IOException;
	void writeEndArray() throws IOException;
	void writeFieldName(String key) throws IOException;
	void writeRawValue(String json) throws IOException;
	void writeObjectField(String key, Object value) throws IOException;
}

class PlaintextJsonGenerator implements DataJsonGenerator {
	private JsonGenerator generator;

	public PlaintextJsonGenerator(JsonGenerator generator) {
		this.generator = generator;
	}

	@Override
	public void begin(String key) throws IOException {
		generator.writeObjectFieldStart(key);
	}

	@Override
	public void end() throws IOException {
		generator.writeEndObject();
	}

	@Override
	public void writeArrayFieldStart(String key) throws IOException {
		generator.writeArrayFieldStart(key);
	}

	@Override
	public void writeObject(Object o) throws IOException {
		generator.writeObject(o);
	}

	@Override
	public void writeEndArray() throws IOException {
		generator.writeEndArray();
	}

	@Override
	public void writeFieldName(String key) throws IOException {
		generator.writeFieldName(key);
	}

	@Override
	public void writeRawValue(String json) throws IOException {
		generator.writeRawValue(json);
	}

	@Override
	public void writeObjectField(String key, Object value) throws IOException {
		generator.writeObjectField(key, value);
	}
}

class EncryptingJsonGenerator implements DataJsonGenerator {
	private JsonGenerator rawGenerator;
	private JsonFactory jsonFactory;
	private String encryptionKey;
	private String secretKey;
	private ByteArrayOutputStream outputStream;
	private JsonGenerator encryptedGenerator;
	private String toplevelKey;

	public EncryptingJsonGenerator(
		JsonGenerator generator, JsonFactory jsonFactory,
		String encryptionKey, String secretKey)
	{
		this.rawGenerator = generator;
		this.jsonFactory = jsonFactory;
		this.encryptionKey = encryptionKey;
		this.secretKey = secretKey;
		this.encryptedGenerator = null;
	}

	@Override
	public void begin(String key) throws IOException {
		// TODO: this is inefficient, reuse
		this.outputStream = new ByteArrayOutputStream();
		this.encryptedGenerator = jsonFactory.createGenerator(outputStream);
		this.toplevelKey = key;
	}

	@Override
	public void end() throws IOException {
		outputStream.close();
		String encryptedJSON = RowEncrypt.encrypt(outputStream.toString(), encryptionKey, secretKey);
		rawGenerator.writeStringField(toplevelKey, encryptedJSON);
	}

	@Override
	public void writeArrayFieldStart(String key) throws IOException {
		encryptedGenerator.writeArrayFieldStart(key);
	}

	@Override
	public void writeObject(Object o) throws IOException {
		encryptedGenerator.writeObject(o);
	}

	@Override
	public void writeEndArray() throws IOException {
		encryptedGenerator.writeEndArray();
	}

	@Override
	public void writeFieldName(String key) throws IOException {
		encryptedGenerator.writeFieldName(key);
	}

	@Override
	public void writeRawValue(String json) throws IOException {
		encryptedGenerator.writeRawValue(json);
	}

	@Override
	public void writeObjectField(String key, Object value) throws IOException {
		encryptedGenerator.writeObjectField(key, value);
	}
}

