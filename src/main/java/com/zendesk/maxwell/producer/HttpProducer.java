package com.zendesk.maxwell.producer;

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.Json;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.nio.charset.Charset;

/**
 * HttpProducer
 *
 * Default produces messages as HTTP POST requests with `Content-Type: application/json; charset=UTF-8`.
 *
 * To modify the message-body (potentially changing Content-Type), and/or modify headers and authentication,
 * see HttpProducerConfiguration.
 *
 * See MaxwellConfig for implemented options.
 */
public class HttpProducer extends AbstractProducer {

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

        request.execute(); // throws error on 300 and above
        lastRequest = request;

        this.context.setPosition(r);
    }

    public GenericUrl getRequestUrl() { return requestUrl; }

    // useful for testing.
    protected HttpRequest lastRequest() { return lastRequest; }
}
