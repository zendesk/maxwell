package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import org.apache.commons.codec.binary.Base64;

interface DataJsonGenerator {
	// A wrapper around JsonGenerator for the `data` (and `old`) payload in a RowMap,
	// in order to support encryption.

	// The plaintext implementation just forwards writes, while the
	// encrypted implementation writes to a local buffer and
	// writes an encrypted contents on end

	// outer object
	void begin(String key, ByteArrayOutputStream bos) throws IOException;
	void set(String secret_key) throws NoSuchAlgorithmException;
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
	public void set(String secret_key){}

	@Override
	public void begin(String key, ByteArrayOutputStream bos) throws IOException {
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
	private String secretKey;
	private byte[] initVector;
	private ByteArrayOutputStream outputStream;
	private JsonGenerator encryptedGenerator;
	private String toplevelKey;

	public EncryptingJsonGenerator(
		JsonGenerator generator, JsonFactory jsonFactory)
	{
		this.rawGenerator = generator;
		this.jsonFactory = jsonFactory;
		this.encryptedGenerator = null;
	}

	@Override
	public void begin(String key, ByteArrayOutputStream bos) throws IOException {
		this.outputStream = bos;
		this.encryptedGenerator = jsonFactory.createGenerator(outputStream);
		this.encryptedGenerator.writeStartObject();
		this.toplevelKey = key;
	}

	@Override
	public void end() throws IOException {
		encryptedGenerator.writeEndObject();
		encryptedGenerator.flush();
		String encryptedJSON = RowEncrypt.encrypt(outputStream.toString(), secretKey, initVector);
		rawGenerator.writeStringField(toplevelKey, encryptedJSON);
		rawGenerator.writeStringField("init_vector", Base64.encodeBase64String(initVector));
		outputStream.reset();
		outputStream.close();
	}

	@Override
	public void set(String secret_key) throws NoSuchAlgorithmException{
		this.secretKey = secret_key;
		SecureRandom randomSecureRandom = SecureRandom.getInstance("SHA1PRNG");
		byte[] iv = new byte[16];
		randomSecureRandom.nextBytes(iv);
		this.initVector = iv;

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
