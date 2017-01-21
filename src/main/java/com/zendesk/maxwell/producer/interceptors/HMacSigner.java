package com.zendesk.maxwell.producer.interceptors;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.tomitribe.auth.signatures.Signature;
import org.tomitribe.auth.signatures.Signer;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Provides HMAC authentication with default parameters per RFC 2617.
 * Useful for implementing webservers with machine to machine communication.
 *
 *  sign request using method, url, 'Date' and 'digest' headers (default convention).
 */
public class HMacSigner implements HttpExecuteInterceptor {

    static final Charset CHARSET = Charset.forName("UTF-8");
    private final Signer signer;

    public HMacSigner(String alias, String secret) {

        signer = new Signer(
                new SecretKeySpec(secret.getBytes(CHARSET), "HmacSHA256"),
                new Signature(alias, "hmac-sha256", null, "(request-target)", "Date", "digest")
        );
    }

    @Override
    public void intercept(HttpRequest request) throws IOException {
        HttpHeaders headers = request.getHeaders();

        Map<String,String> hmacHeaders = new HashMap<>();
        hmacHeaders.put("Date", headers.getDate());
        hmacHeaders.put("digest", (String)headers.get("digest"));

        Signature sig = signer.sign(
                request.getRequestMethod(),
                request.getUrl().getRawPath(),
                hmacHeaders
        );

        // add new headers and authorization to request headers
        headers.setAuthorization(sig.toString());
    }
}


// EXAMPLE: example of validating request on receiving web server.
class ExampleHmacHandler implements HttpHandler {
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
