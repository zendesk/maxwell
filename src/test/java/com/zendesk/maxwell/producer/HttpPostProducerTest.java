package com.zendesk.maxwell.producer;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;

import org.junit.Test;
import static org.junit.Assert.*;

public class HttpPostProducerTest {

    @Test
    public void TestPostRequestWithHmac() throws Exception {
        String payload = "{\"myKey\": \"myVal\"}";
        String hmacSecret = "mysecret";
        String hmacDigest = "b113e5d7c0792017edaf8ab1a43d3522d848a0cc";

        HttpRequest request = null;

        request = HttpPostProducer.buildRequest(
                new MockHttpTransport().createRequestFactory(),
                HttpTesting.SIMPLE_GENERIC_URL,
                payload,
                hmacSecret
        );

        // must calculate correct HMAC digest
        assertEquals(request.getHeaders().get(HttpPostProducer.HTTP_HMAC_HEADER), hmacDigest);
    }

    @Test
    public void TestPostRequestWithNullHmac() throws Exception {
        String payload = "{\"myKey\": \"myVal\"}";

        HttpRequest request = null;

        request = HttpPostProducer.buildRequest(
                new MockHttpTransport().createRequestFactory(),
                HttpTesting.SIMPLE_GENERIC_URL,
                payload,
                null
        );

        // must support `application/json` media type.
        assertEquals(request.getHeaders().getContentType(), HttpPostProducer.HTTP_CONENT_TYPE_HEADER);
    }

}
