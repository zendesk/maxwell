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

	// The plaintext implementation just forwards access, while the
	// encrypted implementation writes to a local buffer and
	// writes an encrypted contents on end

	// outer object
	JsonGenerator begin() throws IOException;
	void end(EncryptionContext context) throws IOException;
}

class PlaintextJsonGenerator implements DataJsonGenerator {
	private JsonGenerator generator;

	public PlaintextJsonGenerator(JsonGenerator generator) {
		this.generator = generator;
	}

	@Override
	public JsonGenerator begin() throws IOException {
		return generator;
	}

	@Override
	public void end(EncryptionContext ctx) throws IOException {
	}
}

class EncryptionContext {
	String secretKey;
	byte[] iv;

	EncryptionContext(String secretKey, byte[] iv) {
		this.secretKey = secretKey;
		this.iv = iv;
	}

	public static EncryptionContext create(String secretKey) throws NoSuchAlgorithmException {
		SecureRandom randomSecureRandom = SecureRandom.getInstance("SHA1PRNG");
		byte[] iv = new byte[16];
		randomSecureRandom.nextBytes(iv);
		return new EncryptionContext(secretKey, iv);
	}
}

class EncryptingJsonGenerator implements DataJsonGenerator {
	final private JsonGenerator rawGenerator;
	final private ByteArrayOutputStream buffer;
	final private JsonGenerator encryptedGenerator;

	public EncryptingJsonGenerator(
		JsonGenerator generator, JsonFactory jsonFactory) throws IOException {
		this.buffer = new ByteArrayOutputStream();
		this.rawGenerator = generator;
		this.encryptedGenerator = jsonFactory.createGenerator(buffer);
	}

	@Override
	public JsonGenerator begin() throws IOException {
		return beginObject();
	}

	@Override
	public void end(EncryptionContext ctx) throws IOException {
		endObject(ctx);
	}

	private JsonGenerator beginObject() throws IOException {
		encryptedGenerator.writeStartObject();
		return encryptedGenerator;
	}

	private void endRaw(EncryptionContext ctx) throws IOException {
		encryptedGenerator.flush();
		String json = buffer.toString();
		buffer.reset();
		writeEncryptedField(json, ctx);
	}

	private void endObject(EncryptionContext ctx) throws IOException {
		encryptedGenerator.writeEndObject();
		endRaw(ctx);
	}

	public void writeEncryptedObject(String rawJson, EncryptionContext ctx) throws IOException {
		rawGenerator.writeStartObject();
		writeEncryptedField(rawJson, ctx);
		rawGenerator.writeEndObject();
	}

	private  void writeEncryptedField(String rawJson, EncryptionContext ctx) throws IOException {
		String encryptedJSON = RowEncrypt.encrypt(rawJson, ctx.secretKey, ctx.iv);
		rawGenerator.writeObjectFieldStart("encrypted");
		rawGenerator.writeStringField("iv", Base64.encodeBase64String(ctx.iv));
		rawGenerator.writeStringField("bytes", encryptedJSON);
		rawGenerator.writeEndObject();
	}
}
