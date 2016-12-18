package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import com.google.api.client.http.GenericUrl;
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

import com.google.api.client.http.javanet.NetHttpTransport;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import java.io.IOException;

public class HttpPostProducer extends AbstractProducer {
    private final GenericUrl httpPostEndPoint;
    static final String HTTP_CONENT_TYPE_HEADER = "application/json";
    static final int maxRetryTimeSeconds = 10;
    private final HttpTransport httpTransport = new NetHttpTransport();

	public HttpPostProducer(MaxwellContext context, String httpPostEndPoint) {
		super(context);
        this.httpPostEndPoint = new GenericUrl(httpPostEndPoint);
	}

	@Override
	public void push(RowMap r) throws Exception {

        Set<String> eventTypeWhilteList = new HashSet<String>(Arrays.asList("insert", "update", "delete"));

        String rowType = r.getRowType();
        if ( eventTypeWhilteList.contains(rowType)) {
            String payload = r.toJSON(outputConfig);
            String database = r.getDatabase();
            String table = r.getTable();

            HttpRequestFactory requestFactory = this.getHttpRequestFactory();

            try {
                this.sendPayload(requestFactory, this.httpPostEndPoint, payload);
            } catch (HttpResponseException err) {
                System.err.println(err.getMessage());
            }

        }

		this.context.setPosition(r);
	}

    public void sendPayload(HttpRequestFactory httpRequestFactory, GenericUrl url, String payload) throws IOException {
        ByteArrayContent content = ByteArrayContent.fromString(HttpPostProducer.HTTP_CONENT_TYPE_HEADER, payload);
        HttpRequest request = httpRequestFactory.buildPostRequest(url, content);

        ExponentialBackOff backoff = this.getExponentialBackoff();
        request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backoff));
        HttpResponse response = request.execute();
    }

    private HttpRequestFactory getHttpRequestFactory() {
        return this.httpTransport.createRequestFactory(new HttpRequestInitializer() {
             public void initialize(HttpRequest request) throws IOException {
                request.getHeaders().setContentType(HttpPostProducer.HTTP_CONENT_TYPE_HEADER);
            }
        });
    }

    private ExponentialBackOff getExponentialBackoff() {
        int maxElapsedTimeMillis = maxRetryTimeSeconds * 1000;
        return new ExponentialBackOff.Builder()
            .setInitialIntervalMillis(500)
            .setMaxElapsedTimeMillis(maxElapsedTimeMillis)
            .setMaxIntervalMillis(6000)
            .setMultiplier(1.5)
            .setRandomizationFactor(0.5)
            .build();
    }

}
