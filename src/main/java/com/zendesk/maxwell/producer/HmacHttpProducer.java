package com.zendesk.maxwell.producer;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.Base64;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.zendesk.maxwell.MaxwellContext;
import org.tomitribe.auth.signatures.Signature;
import org.tomitribe.auth.signatures.Signer;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.security.Key;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;



/**
 * Provides HMAC authentication for message digest and date headers with default parameters per RFC 2617.
 * Useful for implementing webservers with machine to machine communication.
 */
public class HmacHttpProducer extends BackoffHttpProducer {
	static final Charset charset = Charset.forName("UTF-8");

	private final Signer signer;

	public HmacHttpProducer(MaxwellContext context, String endpoint, String alias, String secret) {
		this(context, new NetHttpTransport(), endpoint, alias, secret);
	}

	public HmacHttpProducer(MaxwellContext context, HttpTransport transport, String endpoint, String alias, String secret) {
		super(context, transport, endpoint);

		// default values per RFC
		final Key key = new SecretKeySpec(secret.getBytes(charset), "HmacSHA256");
		final Signature signature = new Signature(alias, "hmac-sha256", null, "date", "digest");
		signer = new Signer(key, signature);
	}

	@Override
	public void signRequest(HttpRequest request, String payload) throws Exception {
		final String dateValue = getDateString();

		final byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload.getBytes(charset));
		final String digestHeader = "SHA256=" + new String(Base64.encodeBase64(digest));

		final Map<String, String> headers = new HashMap<>();
		headers.put("Date", dateValue);
		headers.put("digest", digestHeader);
		Signature sig = signer.sign(request.getRequestMethod(), request.getUrl().getRawPath(), headers);

		// add new headers and authorization to request headers
		request.getHeaders()
				.set("Date", dateValue)
				.set("digest", digestHeader)
				.setAuthorization(sig.toString());
	}

	protected String getDateString() {
		return new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US).format(new Date());
	}


	// EXAMPLE: example of validating request on receiving web server.
	static class ExampleHmacHandler implements HttpHandler {
		private Signer signer;

		@Override
		public void handle(HttpExchange t) throws IOException {
			Map<String, String> headers = new HashMap<>();
			for (Map.Entry<String, List<String>> e : t.getRequestHeaders().entrySet()) {
				StringBuffer buf = new StringBuffer();
				for (String s : e.getValue()) {
					buf.append(s);
				}
				headers.put(e.getKey(), buf.toString());
			}
			Signature expected = Signature.fromString(headers.get("Authorization"));
			Signature computed = signer.sign(t.getRequestMethod(), t.getRequestURI().getRawPath(), headers);

			String response;
			int code;
			if (!expected.getSignature().equals(computed.getSignature())) {
				response = "unauthorized";
				code = 401;
			} else {
				response = "authorized";
				code = 200;
			}
			t.sendResponseHeaders(code, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}
	}
}
