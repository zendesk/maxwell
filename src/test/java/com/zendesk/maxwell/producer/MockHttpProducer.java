package com.zendesk.maxwell.producer;

import com.google.api.client.http.HttpRequest;
//import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.testing.http.*;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.util.List;
import java.util.Stack;

// Helper class for Integration Tests that uses mock transport and has stack of previous requests.
public class MockHttpProducer extends HttpProducer {

    public static final String SAMPLE_URL = "http://requestb.in/ss60d2ss"; // when using real transport.

    private Stack<HttpRequest> previousRequests;

    public MockHttpProducer(MaxwellContext context, HttpProducerConfiguration config) {
        super(context, new MockHttpTransport(), config); // use NetHttpTransport() for requestb.in
        previousRequests = new Stack<>();
    }

    @Override
    public void push(RowMap r) throws Exception {
        super.push(r);
        previousRequests.push(lastRequest());
    }

    public void push(List<RowMap> rs) throws Exception {
        for (RowMap r : rs) {
            this.push(r);
        }
    }

    public Stack<HttpRequest> getLifoRequests() { return previousRequests; }
}
