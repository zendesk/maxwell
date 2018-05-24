package com.zendesk.maxwell.row;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

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


