package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.BinlogPosition;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaCallback implements Callback {
    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaCallback.class);
    private InflightMessageList inflightMessages;
    private final MaxwellContext context;
    private final BinlogPosition position;
    private final boolean isTXCommit;
    private Object data = null;
    private final String key;

    public KafkaCallback(InflightMessageList inflightMessages, BinlogPosition position, boolean isTXCommit, MaxwellContext c, String key, Object data) {
        this.inflightMessages = inflightMessages;
        this.context = c;
        this.position = position;
        this.isTXCommit = isTXCommit;
        this.key = key;
        this.data = data;
    }

    @Override
    public void onCompletion(RecordMetadata md, Exception e) {
        if ( e != null ) {
            LOGGER.error(e.getClass().getSimpleName() + " @ " + position + " -- " + key);
            LOGGER.error(e.getLocalizedMessage());
            if ( e instanceof RecordTooLargeException) {
                LOGGER.error("Considering raising max.request.size broker-side.");
            }
        } else {
            if ( LOGGER.isDebugEnabled()) {
                LOGGER.debug("->  key:" + key + ", partition:" + md.partition() + ", offset:" + md.offset());
                LOGGER.debug("   " + this.data.toString());
                LOGGER.debug("   " + position);
                LOGGER.debug("");
            }
        }
        markCompleted();
    }

    private void markCompleted() {
        if ( isTXCommit ) {
            BinlogPosition newPosition = inflightMessages.completeMessage(position);

            if ( newPosition != null ) {
                context.setPosition(newPosition);
            }
        }
    }

}