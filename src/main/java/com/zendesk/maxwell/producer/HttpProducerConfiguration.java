package com.zendesk.maxwell.producer;

import com.google.api.client.http.*;
import com.google.api.client.util.ExponentialBackOff;

import java.io.IOException;

/*
 * HttpPostProducerConfiguration
 *
 * Requires request URL.
 *
 * To configure your HTTP request factory with a custom header-line and/or message-body modification:
 *  1) provide your own request initializer for fine grain control,
 *  2) -OR- provide backoff strategy and / or request "interceptor".
 *     The request interceptor can be stateful, and allow for adding headers / auth,
 *     and modifying the message-body (e.g. remove fields from JSON, re-encode).
 *
 * By default, a exponential backoff strategy is employed, and available as a static method.
 * An example of adding Basic Authentication is provided in the MaxwellConfig.
 */
public final class HttpProducerConfiguration {

    private final String requestUrl;
    private final HttpRequestInitializer requestInitializer;
    private final ExponentialBackOff.Builder backoffBuilder;
    private final HttpExecuteInterceptor requestInterceptor;

    public HttpProducerConfiguration(String url) {
        this(url, defaultBackoffBuilder(), null);
    }

    public HttpProducerConfiguration(String url, HttpRequestInitializer initializer) {
        this(url, null, null);
    }

    public HttpProducerConfiguration(String url, ExponentialBackOff.Builder backoffBuilder) {
        this(url, backoffBuilder, null);
    }

    public HttpProducerConfiguration(String url, HttpExecuteInterceptor interceptor) {
        this(url, defaultBackoffBuilder(), interceptor);
    }

    public HttpProducerConfiguration(String url,
                                     ExponentialBackOff.Builder backoffBuilder,
                                     HttpExecuteInterceptor interceptor) {
        this.requestUrl = url;
        this.backoffBuilder = backoffBuilder;
        this.requestInterceptor = interceptor;
        this.requestInitializer = createRequestInitializer();
    }

    public String getRequestUrl() { return requestUrl; }

    public HttpRequestInitializer getRequestInitializer() { return requestInitializer; }

    public static ExponentialBackOff.Builder defaultBackoffBuilder() {
        final int MAX_RETRY_SECONDS = 15;

        return new ExponentialBackOff.Builder()
                .setInitialIntervalMillis(500)
                .setMaxElapsedTimeMillis(1000 * MAX_RETRY_SECONDS)
                .setMaxIntervalMillis(6000)
                .setMultiplier(1.5)
                .setRandomizationFactor(0.5);
    }

    private HttpRequestInitializer createRequestInitializer() {
        return new HttpRequestInitializer() {
            @Override
            public void initialize(HttpRequest httpRequest) throws IOException {
                if (backoffBuilder != null) {
                    ExponentialBackOff backoff = backoffBuilder.build();
                    httpRequest.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backoff));
                }

                if (requestInterceptor != null) {
                    httpRequest.setInterceptor(requestInterceptor);
                }

            }
        };
    }
}
