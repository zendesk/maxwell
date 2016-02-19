/**
 * This is simplest kinesis producer, using stream apis provided by aws sdk (and
 * not kpl).
 * Ref: http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-kpl.html
 *
 * Scope/limitations:
 * 1. It puts results synchronously (blocking call) and doesn't usage callback mechanism (as in kpl).
 * 2. It will stop/break execution in cases of exceptions of kind (aws kinesis stream resouce not found, throughput exceeded
 *	exception) and won't attempt to re push the row data.
 * 3. Need to ensure stream is already created in kinesis.
 * 4. Using database name as partition key.
 */

package com.zendesk.maxwell.producer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

import com.amazonaws.services.kinesis.AmazonKinesisClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisProducer extends AbstractProducer {

	static final Logger LOGGER = LoggerFactory.getLogger(KinesisProducer.class);
	static final AtomicLong counter = new AtomicLong();
	private final AmazonKinesisClient kinesis;
	private String stream;

	public KinesisProducer(
			MaxwellContext context,
			String kinesisEndpoint,
			String kinesisStream) {
		super(context);

		this.kinesis = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
		kinesis.setEndpoint(kinesisEndpoint);

		this.stream = kinesisStream;

		if (this.stream == null)
			this.stream = "maxwell";

	}

	@Override
	public void push(RowMap r) throws Exception {

		PutRecordResult putResult = kinesis.putRecord(
				this.stream,
				ByteBuffer.wrap(r.toJSON().toString().getBytes()),
				r.getDatabase(),
				Long.toString(counter.getAndIncrement())
				);
		this.context.setPosition(r);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Success - put result in kinesis stream -> " + putResult.toString());
			LOGGER.debug("Position -> " + r.getPosition());
		}
	}
}
