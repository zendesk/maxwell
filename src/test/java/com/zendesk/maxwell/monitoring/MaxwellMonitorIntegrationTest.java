package com.zendesk.maxwell.monitoring;

import com.zendesk.maxwell.*;
import com.zendesk.maxwell.row.RowMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class MaxwellMonitorIntegrationTest extends MaxwellTestWithIsolatedServer {
	@Test
	public void testHTTPEndpoint() throws Exception {
		String[] sql = {
				"CREATE TABLE `test`.`tingalingaling` ( id int )",
				"insert into `test`.`tingalingaling` set id = 5"
		};


		MaxwellTestSupportCallback cb = new MaxwellTestSupportCallback() {
			@Override
			public void beforeTerminate(MysqlIsolatedServer mysql) throws Exception {
				// Create a neat value object to hold the URL
				URL url = new URL("http://localhost:22222/metrics");

				HttpURLConnection connection = (HttpURLConnection) url.openConnection();

				InputStream responseStream = connection.getInputStream();

				try {
					responseStream.read();
				} catch (Exception e) {
					Assert.assertFalse("exception: " + e.getMessage(), false);
				}
			}
		};
		MaxwellTestSupport.getRowsWithReplicator(server, cb, (config) -> {
			config.httpPort = 22222;
			config.metricsReportingType = "http";
		});

	}
}
