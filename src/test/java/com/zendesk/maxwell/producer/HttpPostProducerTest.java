package com.zendesk.maxwell.producer;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;

import com.zendesk.maxwell.MaxwellTestSupport;
import org.tomitribe.auth.signatures.Signature;

import static org.junit.Assert.*;

public class HttpPostProducerTest {

	// TODO: we need mock MaxwellContext that doesn't try to initialize server.
	public void testHmacHttpProducerSigning() throws Exception {

		HttpTransport t = new MockHttpTransport();

		HmacHttpProducer producer =
				new HmacHttpProducer(
						MaxwellTestSupport.buildContext(0, null, null),
						t,
						HttpTesting.SIMPLE_GENERIC_URL.build(),
						"myKeyId",
						"mysecret"
				) {
					@Override
					protected String getDateString() {
					   return "Tue, 07 Jun 2014 20:51:35 GMT";
					}
				};
		String payload = "{\"myKey\": \"myVal\"}";
		HttpContent c = ByteArrayContent.fromString(Json.MEDIA_TYPE, payload);
		HttpRequest r = t.createRequestFactory().buildPostRequest(HttpTesting.SIMPLE_GENERIC_URL, c);

		producer.signRequest(r, payload);

		Signature got = Signature.fromString(r.getHeaders().getAuthorization());

		assertEquals(got.getSignature(), "todo:calc sha256");
	}

}
