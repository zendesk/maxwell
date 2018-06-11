package com.zendesk.maxwell.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;

public class MaxwellSQSProducer extends AbstractAsyncProducer {

	private AmazonSQSAsync client;
	private String queueUri;

	public MaxwellSQSProducer(MaxwellContext context, String queueUri) {
		super(context);
		this.queueUri = queueUri;
		this.client = AmazonSQSAsyncClientBuilder.defaultClient();
	}

	@Override
	public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
		String value = r.toJSON();
		SendMessageRequest messageRequest = new SendMessageRequest(queueUri, value);
		SQSCallback callback = new SQSCallback(cc, r.getPosition(), value, context);
		client.sendMessageAsync(messageRequest, callback);
	}

}

class SQSCallback implements AsyncHandler<SendMessageRequest, SendMessageResult> {
	public static final Logger logger = LoggerFactory.getLogger(SQSCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private MaxwellContext context;

	public SQSCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String json,
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
	public void onSuccess(SendMessageRequest request, SendMessageResult result) {
		if (logger.isDebugEnabled()) {
			logger.debug("-> Message id:" + result.getMessageId() + ", sequence number:" + result.getSequenceNumber()+"  "+json+"  "+position);
		}
		cc.markCompleted();
	}

}
