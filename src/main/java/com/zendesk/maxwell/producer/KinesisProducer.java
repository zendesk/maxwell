package com.zendesk.maxwell.producer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KinesisProducer extends AbstractProducer {

	static final Logger LOGGER = LoggerFactory.getLogger(KinesisProducer.class);
    static final AtomicLong counter = new AtomicLong();
    private final AmazonKinesisClient kinesis;

	public KinesisProducer(MaxwellContext context, Properties awsClientProperties, String kinesisStream) {
		super(context);

        BasicAWSCredentials awsCred = new BasicAWSCredentials(
                awsClientProperties.getProperty("accessKey"),
                awsClientProperties.getProperty("secretKey")
                );

        this.kinesis = new AmazonKinesisClient(awsCred);

        this.stream = kinesisStream;
        if (this.stream == null)
            this.stream = "maxwell";

	}

	@Override
	public void push(RowMap r) throws Exception {

        try {
            PutRecordResult putResult = kinesis.putRecord(
                    this.stream,
                    r.toJSON(),
                    r.getDatabase(), // Using database name as partition key
                    Long.toString(counter.getAndIncrement())
                    );
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug();
            }
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
        } catch (InvalidArgumentException e) {
            e.printStackTrace();
        } catch (ProvisionedThroughputExceededException e} {
            e.printStackTrace();
        }
	}
}
