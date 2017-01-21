package com.zendesk.maxwell.producer;

import com.google.api.client.http.*;
import com.google.api.client.util.ExponentialBackOff;

import java.io.IOException;

/*
 * Provides HttpRequestInitializer and HttpExecuteInterceptor functionality as options to HttpPostProducer.
 *
 * defaults to exponential backoff if not supplied.
 */
public class HttpPostProducerInitializer implements HttpRequestInitializer {

    private final HttpExecuteInterceptor interceptor; // a custom interceptor.
    private final ExponentialBackOff.Builder backoffBuilder;

    public HttpPostProducerInitializer() {
        this(defaultBackoffBuilder(), null);
    }

    public HttpPostProducerInitializer(ExponentialBackOff.Builder builder) {
        this(builder, null);
    }

    public HttpPostProducerInitializer(HttpExecuteInterceptor inter) {
        this(defaultBackoffBuilder(), inter);
    }

    public HttpPostProducerInitializer(ExponentialBackOff.Builder builder, HttpExecuteInterceptor inter) {
        backoffBuilder = builder;
        interceptor = inter;
    }

    @Override
    public void initialize(HttpRequest request) throws IOException {

        if (backoffBuilder != null) {
            ExponentialBackOff backoff = backoffBuilder.build();
            request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backoff));
        }

        if (interceptor != null) {
            request.setInterceptor(interceptor);
        }
    }

    public static ExponentialBackOff.Builder defaultBackoffBuilder() {
        final int MAX_RETRY_SECONDS = 15;

        return new ExponentialBackOff.Builder()
                .setInitialIntervalMillis(500)
                .setMaxElapsedTimeMillis(1000 * MAX_RETRY_SECONDS)
                .setMaxIntervalMillis(6000)
                .setMultiplier(1.5)
                .setRandomizationFactor(0.5);
    }
}

