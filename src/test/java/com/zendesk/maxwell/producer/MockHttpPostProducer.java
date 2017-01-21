package com.zendesk.maxwell.producer;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.*;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.util.List;
import java.util.Stack;

// Helper class for Integration Tests that uses mock transport and has stack of previous requests.
public class MockHttpPostProducer extends HttpPostProducer {

    static final String EXAMPLE_DATE_STRING = "Sat, 21 Jan 2017 12:25:02 EST";
    static final String SAMPLE_URL = "http://requestb.in/1lw4tln1";
    private Stack<HttpRequest> previousRequests;

    public MockHttpPostProducer(MaxwellContext context, HttpPostProducerInitializer initializer) {
        super(context, SAMPLE_URL, new MockHttpTransport(), initializer);
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

    @Override
    public String getDateString() { return EXAMPLE_DATE_STRING; }
}
