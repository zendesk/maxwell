package com.zendesk.maxwell.producer;

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.Json;
import com.google.api.client.util.Base64;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Produces application/json POST request with Date and SHA-256 digest headers.
 */
public class HttpPostProducer extends AbstractProducer {

    public static final String DIGEST_ALGO = "SHA-256";
    public static final Charset CHARSET = Charset.forName("UTF-8");

    private HttpTransport transport;
    private HttpPostProducerInitializer initializer;
    private HttpRequestFactory requestFactory;
    private final GenericUrl endpoint;

    private HttpRequest lastRequest;

    public HttpPostProducer(MaxwellContext context, String endpoint) {
        this(context, endpoint, new NetHttpTransport(), new HttpPostProducerInitializer());
    }

    public HttpPostProducer(MaxwellContext context, String endpoint, HttpPostProducerInitializer initializer) {
        this(context, endpoint, new NetHttpTransport(), initializer);
    }

    public HttpPostProducer(MaxwellContext context, String endpoint, HttpTransport transport, HttpPostProducerInitializer initializer) {
        super(context);
        this.endpoint = new GenericUrl(endpoint);
        this.transport = transport;
        this.initializer = initializer;

        // init request factory.
        this.requestFactory = transport.createRequestFactory(initializer);
    }

    @Override
    public void push(RowMap r) throws Exception {
        String payload = r.toJSON(outputConfig);
        ByteArrayContent content = ByteArrayContent.fromString(Json.MEDIA_TYPE, payload);
        HttpRequest request = requestFactory.buildPostRequest(endpoint, content);

        // add statndard webhook headers: Content-Length, Date, and digest.
        HttpHeaders headers = request.getHeaders();
        headers.setContentLength(content.getLength());
        headers.setDate(getDateString());
        headers.set("digest", digestHeader(DIGEST_ALGO, CHARSET, payload));

        request.execute(); // throws error on 300 and above
        lastRequest = request;

        this.context.setPosition(r);
    }

    public GenericUrl getEndpoint() { return endpoint; }

    // useful for testing.
    protected HttpRequest lastRequest() { return lastRequest; }

    public String getDateString() {
        return new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US).format(new Date());
    }

    public static String digestHeader(String algo, Charset charset, String payload) throws NoSuchAlgorithmException {
        byte[] digest = MessageDigest.getInstance(algo).digest(payload.getBytes(charset));
        return String.format("%s=%s", algo, new String(Base64.encodeBase64(digest), charset));
    }


}
