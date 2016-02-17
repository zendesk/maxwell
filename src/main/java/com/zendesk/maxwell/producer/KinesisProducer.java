/**
 * This is simplest kinesis producer, using stream apis provided by aws sdk (and
 * not kpl).
 * Ref: http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-kpl.html
 *
 * Scope/limitations:
 * 1. It puts results synchronously (blocking call) and doesn't usage callback mechanism (as in kpl).
 * 2. It will stop/break execution in cases of exceptions of kind (aws kinesis stream resouce not found, throughput exceeded
 *    exception) and won't attempt to re push the row data.
 * 3. Need to ensure stream is already created in kinesis.
 * 4. Using database name as partition key.
 */

package com.zendesk.maxwell.producer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.sql.SQLException;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.*;

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
            String awsAccessKey,
            String awsSecretKey,
            String kinesisStream) {
        super(context);

        BasicAWSCredentials awsCred = new BasicAWSCredentials(
                awsAccessKey,
                awsSecretKey
                );

        this.kinesis = new AmazonKinesisClient(awsCred);
        kinesis.setEndpoint(kinesisEndpoint);

        this.stream = kinesisStream;

        if (this.stream == null)
            this.stream = "maxwell";

    }

    @Override
    public void push(RowMap r) throws Exception {

        try {
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
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
        } catch (InvalidArgumentException e) {
            e.printStackTrace();
        } catch (ProvisionedThroughputExceededException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
