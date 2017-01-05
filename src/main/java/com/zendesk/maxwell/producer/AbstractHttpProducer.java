package com.zendesk.maxwell.producer;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.json.Json;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.util.ExponentialBackOff;

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.io.IOException;

public abstract class AbstractHttpProducer extends AbstractProducer implements HttpRequestInitializer {

	protected final GenericUrl endpoint;
	protected final HttpRequestFactory requestFactory;
	private HttpRequest lastRequest;

	public AbstractHttpProducer(MaxwellContext context, String endpoint) {
		this(context, new NetHttpTransport(), endpoint);
	}

	public AbstractHttpProducer(MaxwellContext context, HttpTransport transport, String endpoint) {
		super(context);
		this.endpoint = new GenericUrl(endpoint);
		this.requestFactory = transport.createRequestFactory(this);
	}

	@Override
	public void push(RowMap r) throws Exception {
		String payload = r.toJSON(outputConfig);
		ByteArrayContent content = ByteArrayContent.fromString(Json.MEDIA_TYPE, payload);
		HttpRequest request = requestFactory.buildPostRequest(endpoint, content);
		request.getHeaders().setContentLength(content.getLength());

		signRequest(request, payload);
		request.execute(); // throws error on 300 and above

		lastRequest = request;
		this.context.setPosition(r);
	}

	// pointer to the last request. useful for testing w/o Dep. Injection framework.
	protected HttpRequest getLastRequest() {
		return lastRequest;
	}

	abstract protected void signRequest(HttpRequest request, String payload) throws Exception;

}

