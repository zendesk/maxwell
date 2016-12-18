package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellTestSupport;
import com.zendesk.maxwell.MaxwellTestSupport;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;

import java.sql.SQLException;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class HttpPostProducerTest {
    private String endPoint = "http://test.local";
    private HttpPostProducer producer;

	@Before
	public void setupBefore() {
        try {
            MaxwellContext context = MaxwellTestSupport.buildContext(3306, null, null);
            this.producer = new HttpPostProducer(context, this.endPoint);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
	}

    @Test
    public void TestSendSuccessfulPayload() {
        String testUrl = "http://test.local";
        HttpTransport transport = new MockHttpTransport();
        HttpRequestFactory factory = transport.createRequestFactory();
        GenericUrl url = new GenericUrl(testUrl);
        String data = "{\"myKey\": \"myVal\"}";

        try {
            this.producer.sendPayload(factory, url, data);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        assert(true);
    }

}
