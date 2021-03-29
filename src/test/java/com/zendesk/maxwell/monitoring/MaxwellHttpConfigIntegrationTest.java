package com.zendesk.maxwell.monitoring;

import com.zendesk.maxwell.*;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.*;

public class MaxwellHttpConfigIntegrationTest extends MaxwellTestWithIsolatedServer {

	private HttpResponse<String> getConfig() throws IOException, InterruptedException {
		HttpClient client = HttpClient.newHttpClient();
		HttpRequest request = HttpRequest.newBuilder()
    .uri(URI.create("http://localhost:22222/config"))
		.build();
		return client.send(request, HttpResponse.BodyHandlers.ofString());
	}

	private HttpResponse<String> updateConfig(String method, String filter) throws IOException, InterruptedException {
		HttpClient client = HttpClient.newHttpClient();
		HttpRequest request = HttpRequest.newBuilder()
		.method(method, HttpRequest.BodyPublishers.ofString(String.format("{\"filter\":\"%s\"}", filter)))
    .uri(URI.create("http://localhost:22222/config"))
		.build();
		return client.send(request, HttpResponse.BodyHandlers.ofString());
	}

	private boolean probeHTTP() {
		try {
			// Create a neat value object to hold the URL
			URL url = new URL("http://localhost:22222/config");

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			InputStream responseStream = connection.getInputStream();

			responseStream.read();
			return true;
		} catch (Exception e) {
			return false;
		}

	}
	@Test
	public void testHTTPEndpoint() throws Exception {
		MaxwellTestSupportCallback cb = new MaxwellTestSupportCallback() {
			@Override
			public void beforeTerminate(MysqlIsolatedServer mysql) throws Exception {
				for ( int i = 0 ; i < 5; i++ ) {
					if ( probeHTTP() ) {
						// test get
						HttpResponse<String> cfg = getConfig();
						Assert.assertEquals(cfg.statusCode(), 200);
						Assert.assertEquals(cfg.body(), "{\"filter\":null}\n");

						// test valid post
						updateConfig("PUT", "exclude: *.*, blacklist: bad_db.*");
						cfg = getConfig();
						Assert.assertEquals(cfg.statusCode(), 200);
						Assert.assertEquals(cfg.body(), "{\"filter\":\"exclude: *.*, blacklist: bad_db.*\"}\n");

						// test valid put
						updateConfig("PUT", "exclude: *.*, blacklist: bad_db.*");
						cfg = getConfig();
						Assert.assertEquals(cfg.statusCode(), 200);
						Assert.assertEquals(cfg.body(), "{\"filter\":\"exclude: *.*, blacklist: bad_db.*\"}\n");

						// test valid patch
						updateConfig("PATCH", "exclude: *.*, blacklist: bad_db.*");
						cfg = getConfig();
						Assert.assertEquals(cfg.statusCode(), 200);
						Assert.assertEquals(cfg.body(), "{\"filter\":\"exclude: *.*, blacklist: bad_db.*\"}\n");

						// test invalid patch
						cfg = updateConfig("PATCH", "\"{");
						Assert.assertEquals(cfg.statusCode(), 400);
						Assert.assertEquals(cfg.body(), "{\"error\":{\"code\":400,\"message\":\"error processing body: Unexpected character ('{' (code 123)): was expecting comma to separate Object entries\\n at [Source: (String)\\\"{\\\"filter\\\":\\\"\\\"{\\\"}\\\"; line: 1, column: 14]\"}}\n");
						cfg = getConfig();
						Assert.assertEquals(cfg.body(), "{\"filter\":\"exclude: *.*, blacklist: bad_db.*\"}\n");

						// test invalid filter in patch
						cfg = updateConfig("PATCH", "bl: *.*");
						Assert.assertEquals(cfg.statusCode(), 400);
						Assert.assertEquals(cfg.body(), "{\"error\":{\"code\":400,\"message\":\"invalid filter: Unknown filter keyword: bl\"}}\n");
						cfg = getConfig();
						Assert.assertEquals(cfg.body(), "{\"filter\":\"exclude: *.*, blacklist: bad_db.*\"}\n");
						return;
					}
					try { Thread.sleep(1000); } catch ( InterruptedException e ) {
					}
				}
				Assert.assertFalse("failed to connect to http server after 5 seconds!", false);
			}
		};
		MaxwellTestSupport.getRowsWithReplicator(server, cb, (config) -> {
			config.httpPort = 22222;
			config.enableHttpConfig = true;
		});

	}
}
