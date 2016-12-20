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
import org.apache.commons.codec.binary.Hex;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import java.io.IOException;

public class HttpPostProducer extends AbstractProducer {
    static final String HTTP_HMAC_HEADER = "x-signature";
    static final String HTTP_CONENT_TYPE_HEADER = "application/json";
    static final String HMAC_ALG = "HmacSHA1";

    private static final int maxRetryTimeSeconds = 15;
    private final GenericUrl httpPostEndPoint;
    private final String httpPostHmacSecret;
    private final HttpTransport httpTransport = new NetHttpTransport();

	public HttpPostProducer(MaxwellContext context, String httpPostEndPoint, String httpPostHmacSecret) {
		super(context);
        this.httpPostEndPoint = new GenericUrl(httpPostEndPoint);
        this.httpPostHmacSecret = httpPostHmacSecret;
	}

	@Override
	public void push(RowMap r) throws Exception {

        Set<String> eventTypeWhiteList = new HashSet<String>(Arrays.asList("insert", "update", "delete"));

        String rowType = r.getRowType();
        if ( eventTypeWhiteList.contains(rowType) ) {
            String payload = r.toJSON(outputConfig);

            HttpRequest request = buildRequest(
                this.getHttpRequestFactory(),
                this.httpPostEndPoint,
                payload,
                this.httpPostHmacSecret);

            // throws error on 300 and above
            HttpResponse response = request.execute();
        }

		this.context.setPosition(r);
	}

    static HttpRequest buildRequest(HttpRequestFactory httpRequestFactory, GenericUrl url, String payload, String secret) throws Exception {

        ByteArrayContent content = ByteArrayContent.fromString(HttpPostProducer.HTTP_CONENT_TYPE_HEADER, payload);
        HttpRequest request = httpRequestFactory.buildPostRequest(url, content);

        HttpHeaders headers = request.getHeaders()
                .setContentType(HTTP_CONENT_TYPE_HEADER)
                .setContentLength(content.getLength());

        if (secret != null) {
            String digest = generateHmacSha1(payload, secret);
            headers.set(HTTP_HMAC_HEADER, digest);
        }
        request.setHeaders(headers);

        ExponentialBackOff backoff = getExponentialBackoff();
        request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backoff));
        return request;
    }

    private HttpRequestFactory getHttpRequestFactory() {
        return this.httpTransport.createRequestFactory(new HttpRequestInitializer() {
             public void initialize(HttpRequest request) throws IOException {}
        });
    }

    private static ExponentialBackOff getExponentialBackoff() {
        int maxElapsedTimeMillis = maxRetryTimeSeconds * 1000;
        return new ExponentialBackOff.Builder()
            .setInitialIntervalMillis(500)
            .setMaxElapsedTimeMillis(maxElapsedTimeMillis)
            .setMaxIntervalMillis(6000)
            .setMultiplier(1.5)
            .setRandomizationFactor(0.5)
            .build();
    }

    private static String generateHmacSha1(String value, String key) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(key.getBytes("UTF-8"), HMAC_ALG);
        Mac mac = Mac.getInstance(HMAC_ALG);
        mac.init(keySpec);

        byte[] rawHmac = mac.doFinal(value.getBytes("UTF-8"));
        byte[] hexHmac = new Hex().encode(rawHmac);

        return new String(hexHmac, "UTF-8");
    }

}
