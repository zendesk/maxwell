package com.zendesk.maxwell.row;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.nio.charset.Charset;

public class RowEncrypt {
	private final static String TEXT_ENCODING = "UTF-8";
	private final static String BYTE_ENCODING = "ASCII";

	public static String encrypt(String value, String secretKey, byte[] initVector) throws Exception {
		IvParameterSpec ivSpec = new IvParameterSpec(initVector);
		Cipher cipher = getCipher();
		cipher.init(Cipher.ENCRYPT_MODE, loadKey(secretKey), ivSpec);

		byte[] encrypted = cipher.doFinal(value.getBytes(TEXT_ENCODING));
		return Base64.encodeBase64String(encrypted);
	}

	public static String decrypt(String value, String secretKey, String initVector) throws Exception {
		IvParameterSpec ivSpec = new IvParameterSpec(base64Decode(initVector));
		Cipher cipher = getCipher();
		cipher.init(Cipher.DECRYPT_MODE, loadKey(secretKey), ivSpec);

		return new String(cipher.doFinal(base64Decode(value)), Charset.forName(TEXT_ENCODING));
	}

	private static Cipher getCipher() throws Exception {
		return Cipher.getInstance("AES/CBC/PKCS5PADDING");
	}

	private static SecretKeySpec loadKey(String secretKey) throws IOException {
		return new SecretKeySpec(secretKey.getBytes(TEXT_ENCODING), "AES");
	}

	private static byte[] base64Decode(String value) throws IOException {
		return Base64.decodeBase64(value.getBytes(BYTE_ENCODING));
	}
}
