package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.commons.codec.binary.Base64;

interface DataJsonGenerator {
	// A wrapper around JsonGenerator for the `data` (and `old`) payload in a RowMap,
	// in order to support encryption.

	// The plaintext implementation just forwards access, while the
	// encrypted implementation writes to a local buffer and
	// writes an encrypted contents on end

	// outer object
	JsonGenerator begin() throws IOException;
	void end(EncryptionContext context) throws Exception;
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
	public void end(EncryptionContext ctx) throws Exception {
		endObject(ctx);
	}

	private JsonGenerator beginObject() throws IOException {
		encryptedGenerator.writeStartObject();
		return encryptedGenerator;
	}

	private void endRaw(EncryptionContext ctx) throws Exception {
		encryptedGenerator.flush();
		String json = buffer.toString();
		buffer.reset();
		writeEncryptedField(json, ctx);
	}

	private void endObject(EncryptionContext ctx) throws Exception {
		encryptedGenerator.writeEndObject();
		endRaw(ctx);
	}

	public void writeEncryptedObject(String rawJson, EncryptionContext ctx) throws Exception {
		rawGenerator.writeStartObject();
		writeEncryptedField(rawJson, ctx);
		rawGenerator.writeEndObject();
	}

	private  void writeEncryptedField(String rawJson, EncryptionContext ctx) throws Exception {
		String encryptedJSON = RowEncrypt.encrypt(rawJson, ctx.secretKey, ctx.iv);
		rawGenerator.writeObjectFieldStart("encrypted");
		rawGenerator.writeStringField("iv", Base64.encodeBase64String(ctx.iv));
		rawGenerator.writeStringField("bytes", encryptedJSON);
		rawGenerator.writeEndObject();
	}
}
