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
	void begin(String key) throws IOException;
	void writeMetadata(EncryptionContext context) throws IOException;
	void end(EncryptionContext context) throws IOException;

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
	public void writeMetadata(EncryptionContext ctx) {
	}

	@Override
	public void begin(String key) throws IOException {
		generator.writeObjectFieldStart(key);
	}

	@Override
	public void end(EncryptionContext ctx) throws IOException {
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

class EncryptionContext {
	String secretKey;
	byte[] iv;

	EncryptionContext(String secretKey, byte[] iv) {
		this.secretKey = secretKey;
		this.iv = iv;
	}
}


class EncryptingJsonGenerator implements DataJsonGenerator {
	final private JsonGenerator rawGenerator;
	final private JsonFactory jsonFactory;
	final private ByteArrayOutputStream buffer;
	final private JsonGenerator encryptedGenerator;
	private String toplevelKey;

	public EncryptingJsonGenerator(
		JsonGenerator generator, JsonFactory jsonFactory) throws IOException {
		this.buffer = new ByteArrayOutputStream();
		this.rawGenerator = generator;
		this.jsonFactory = jsonFactory;
		this.encryptedGenerator = jsonFactory.createGenerator(buffer);
	}

	@Override
	public void begin(String key) throws IOException {
		this.encryptedGenerator.writeStartObject();
		this.toplevelKey = key;
	}

	@Override
	public void end(EncryptionContext ctx) throws IOException {
		encryptedGenerator.writeEndObject();
		encryptedGenerator.flush();
		String encryptedJSON = RowEncrypt.encrypt(buffer.toString(), ctx.secretKey, ctx.iv);
		rawGenerator.writeStringField(toplevelKey, encryptedJSON);
		buffer.reset();
	}

	public static EncryptionContext createEncryptionContext(String secretKey) throws IOException,NoSuchAlgorithmException {
		SecureRandom randomSecureRandom = SecureRandom.getInstance("SHA1PRNG");
		byte[] iv = new byte[16];
		randomSecureRandom.nextBytes(iv);
		return new EncryptionContext(secretKey, iv);
	}

	public static void writeMetadata(JsonGenerator rawGenerator, EncryptionContext ctx) throws IOException {
		rawGenerator.writeStringField("init_vector", Base64.encodeBase64String(ctx.iv));
	}

	@Override
	public void writeMetadata(EncryptionContext ctx) throws IOException {
		EncryptingJsonGenerator.writeMetadata(rawGenerator, ctx);
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
