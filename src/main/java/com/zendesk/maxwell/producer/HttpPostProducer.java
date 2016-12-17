package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class HttpPostProducer extends AbstractProducer {
	public HttpPostProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {

        Set<String> eventTypeWhilteList = new HashSet<String>(Arrays.asList("insert", "update", "delete"));

        String rowType = r.getRowType();
        if ( eventTypeWhilteList.contains(rowType)) {
            String payload = r.toJSON(outputConfig);
            String database = r.getDatabase();
            String table = r.getTable();

            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost("http://requestb.in/1k1i3n81");
            httpPost.addHeader("Content-Type", "application/json");
            StringEntity params = new StringEntity(payload);
            httpPost.setEntity(params);
            CloseableHttpResponse response = httpClient.execute(httpPost);
            int status = response.getStatusLine().getStatusCode();

            System.out.println(payload);
            String statusMessage = String.format("Got status %s", status);
            System.out.println(statusMessage);


        } else {
            String message = String.format("Event type %s ignoring ...", rowType);
            System.out.println(message);
        }

		this.context.setPosition(r);
	}
}
