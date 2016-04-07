package com.zendesk.maxwell.producer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

class PubsubCallback implements FutureCallback<HttpResponse> {
	private final MaxwellContext context;
	private final RowMap rowMap;

	public PubsubCallback(RowMap r, MaxwellContext c) {
		this.context = c;
		this.rowMap = r;
	}

	@Override
	public void completed(HttpResponse httpResponse) {
		try {
			if (this.rowMap.isTXCommit()) {
				this.context.setPosition(this.rowMap.getPosition());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void failed(Exception e) {
		e.printStackTrace();
	}

	@Override
	public void cancelled() {

	}
}

public class PubsubProducer extends AbstractProducer {
	static final long REFRESH_ACCESS_TOKEN_INTERVAL = 90 * 60 * 1000;
	private final CloseableHttpAsyncClient httpClient;
	private String accessToken = "";
	private long accessTokenRefreshedTime;
	private final String projectId;
	private final String topic;

	public PubsubProducer(MaxwellContext context, String pubsubProjectId, String pubsubTopic) {
		super(context);

		this.httpClient = HttpAsyncClients.createDefault();
		this.httpClient.start();

		this.refreshAccessToken();

		this.projectId = pubsubProjectId;
		this.topic = pubsubTopic;
	}

	@Override
	public void push(RowMap r) throws Exception {
		// construct pub sub body that looks like:
		// {
		// 	  "messages": [{
		// 		  "data": "base 64 encoded data in json"
		// 	  }]
		// }
		String data = Base64.encodeBase64String(r.toJSON().getBytes(StandardCharsets.UTF_8));

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonFactory jsonFactory = new JsonFactory();
		JsonGenerator generator = jsonFactory.createGenerator(baos);

		generator.writeStartObject();

		generator.writeArrayFieldStart("messages");

		generator.writeStartObject();
		generator.writeStringField("data", data);
		generator.writeEndObject();

		generator.writeEndArray();

		generator.writeEndObject();

		generator.flush();

		String publishUrl = "https://pubsub.googleapis.com/v1/projects/" + this.projectId + "/topics/" +
				this.topic + ":publish";
		HttpPost request = new HttpPost(publishUrl);
		request.addHeader("Authorization", "Bearer " + this.accessToken);
		request.addHeader("Content-Type", "application/json");
		StringEntity entity = new StringEntity(baos.toString(StandardCharsets.UTF_8.toString()));
		request.setEntity(entity);

		this.httpClient.execute(request, new PubsubCallback(r, this.context));

		if (System.currentTimeMillis() > this.accessTokenRefreshedTime + REFRESH_ACCESS_TOKEN_INTERVAL) {
			this.refreshAccessToken();
		}
	}

	private void refreshAccessToken() {
		try {
			this.accessToken = GoogleCredential.getApplicationDefault().getAccessToken();
			this.accessTokenRefreshedTime = System.currentTimeMillis();
		} catch (Throwable throwable) {
			throwable.printStackTrace();

			this.accessToken = "";
		}
	}
}
