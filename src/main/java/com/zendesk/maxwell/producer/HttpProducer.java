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
 *
 * Produces messages as HTTP POST requests.
 *
 * By default, requests are sent UTF-8 with Content-Type: `application/json`, with Date and Digest headers.
 *
 * To modify the message-body (potentially changing Content-Type), and/or modify headers and authentication,
 * see HttpProducerConfiguration.
 *
 * See MaxwellConfig for additional options.
 */
public class HttpProducer extends AbstractProducer {

    public static final String  DEFAULT_DIGEST_ALGO = "SHA-256";
    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private final GenericUrl requestUrl;
    private HttpRequestFactory requestFactory;

    private HttpRequest lastRequest;

    public HttpProducer(MaxwellContext context, HttpProducerConfiguration config) {
        this(context, new NetHttpTransport(), config);
    }

    public HttpProducer(MaxwellContext context, HttpTransport transport, HttpProducerConfiguration config) {
        super(context);
        requestUrl = new GenericUrl(config.getRequestUrl());
        requestFactory = transport.createRequestFactory(config.getRequestInitializer());
    }

    @Override
    public void push(RowMap r) throws Exception {
        String payload = r.toJSON(outputConfig);
        ByteArrayContent content = ByteArrayContent.fromString(Json.MEDIA_TYPE, payload);
        HttpRequest request = requestFactory.buildPostRequest(requestUrl, content);

        HttpHeaders headers = request.getHeaders();
        headers.setContentLength(content.getLength());
        headers.setDate(getDateString());
        headers.set("digest", digestHeader(DEFAULT_DIGEST_ALGO, DEFAULT_CHARSET, payload));

        request.execute(); // throws error on 300 and above
        lastRequest = request;

        this.context.setPosition(r);
    }

    public GenericUrl getRequestUrl() { return requestUrl; }

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
