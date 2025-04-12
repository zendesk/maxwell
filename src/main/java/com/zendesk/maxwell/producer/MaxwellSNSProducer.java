package com.zendesk.maxwell.producer;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellSNSPartitioner;
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
	private MaxwellSNSPartitioner partitioner;

	public MaxwellSNSProducer(MaxwellContext context, String topic) {
		super(context);
		this.topic = topic;
		this.client = AmazonSNSAsyncClientBuilder.defaultClient();
		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		this.partitioner = new MaxwellSNSPartitioner(partitionKey, partitionColumns, partitionFallback);
	}

	public void setClient(AmazonSNSAsync client) {
		this.client = client;
	}

	@Override
	public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
		String value = r.toJSON(outputConfig);
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
			String key = this.partitioner.getSNSKey(r);
			publishRequest.setMessageGroupId(key);
		}
		publishRequest.setMessageAttributes(messageAttributes);

		SNSCallback callback = new SNSCallback(cc, r.getNextPosition(), value,
			r.getDatabase(), r.getTable(), r.getRowIdentity().toConcatString(), r.getApproximateSize(), context);
		client.publishAsync(publishRequest, callback);
	}

}

class SNSCallback implements AsyncHandler<PublishRequest, PublishResult> {
	public static final Logger logger = LoggerFactory.getLogger(SNSCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private final String database;
	private final String table;
	private final String pk;
	private final long approximateRowSize;
	private MaxwellContext context;

	public SNSCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String json,
			String database, String table, String pk, long approximateRowSize,
			MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.json = json;
		this.context = context;
		this.database = database;
		this.table = table;
		this.pk = pk;
		this.approximateRowSize = approximateRowSize;
	}

	@Override
	public void onError(Exception t) {
		logger.error(t.getClass().getSimpleName() + " @ " + position + " -- ");
		logger.error("Database:" + database + ", Table:" + table + ", PK:" + pk + ", Approx Size:" + Long.toString(approximateRowSize));
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
