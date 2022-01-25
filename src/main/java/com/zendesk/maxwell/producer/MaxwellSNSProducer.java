package com.zendesk.maxwell.producer;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MaxwellSNSProducer extends AbstractAsyncProducer {

	private AmazonSNSAsync client;
	private String topic;
	private String[] stringFelds = {"database", "table"};
	private String[] numberFields = {"ts", "xid"};

	public MaxwellSNSProducer(MaxwellContext context, String topic) {
		super(context);
		this.topic = topic;
		this.client = AmazonSNSAsyncClientBuilder.defaultClient();
	}

	public void setClient(AmazonSNSAsync client) {
		this.client = client;
	}

	@Override
	public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
		String value = r.toJSON();
		// Publish a message to an Amazon SNS topic.
		final PublishRequest publishRequest = new PublishRequest(topic, value);
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();

		final String configuredAttributes = context.getConfig().snsAttrs;
		if (configuredAttributes != null) {
			for (String element: configuredAttributes.split(",")) {
				switch (element) {
					case "database":
						messageAttributes.put(
							"database",
							new MessageAttributeValue().withDataType("String").withStringValue(r.getDatabase())
						);
						break;
					case "table":
						messageAttributes.put(
							"table",
							new MessageAttributeValue().withDataType("String").withStringValue(r.getTable())
						);
						break;
				}
			}
		}

		if ( topic.endsWith(".fifo")) {
			publishRequest.setMessageGroupId(r.getDatabase());
		}
		publishRequest.setMessageAttributes(messageAttributes);

		SNSCallback callback = new SNSCallback(cc, r.getNextPosition(), value, context);
		client.publishAsync(publishRequest, callback);
	}

}

class SNSCallback implements AsyncHandler<PublishRequest, PublishResult> {
	public static final Logger logger = LoggerFactory.getLogger(SNSCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private MaxwellContext context;

	public SNSCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String json,
			MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.json = json;
		this.context = context;
	}

	@Override
	public void onError(Exception t) {
		logger.error(t.getClass().getSimpleName() + " @ " + position + " -- ");
		logger.error(t.getLocalizedMessage());
		logger.error("Exception during put", t);

		if (!context.getConfig().ignoreProducerError) {
			context.terminate(new RuntimeException(t));
		} else {
			cc.markCompleted();
		}
	};

	@Override
	public void onSuccess(PublishRequest request, PublishResult result) {
		if (logger.isDebugEnabled()) {
			logger.debug("-> MessageId: {}", result.getMessageId());
		}
		cc.markCompleted();
	}

}
