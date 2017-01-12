package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellKafkaPartitioner;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;


public abstract class AbstractKafkaProducer extends AbstractAsyncProducer {
    static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaProducer.class);

    protected InflightMessageList inflightMessages;
    protected String topic;
    protected String ddlTopic;
    protected MaxwellKafkaPartitioner partitioner;
    protected MaxwellKafkaPartitioner ddlPartitioner;
    protected RowMap.KeyFormat keyFormat;

    public AbstractKafkaProducer(MaxwellContext context, String kafkaTopic) {
        super(context);

        this.topic = kafkaTopic;
        if ( this.topic == null ) {
            this.topic = "maxwell";
        }

        String hash = context.getConfig().kafkaPartitionHash;
        String partitionKey = context.getConfig().kafkaPartitionKey;
        String partitionColumns = context.getConfig().kafkaPartitionColumns;
        String partitionFallback = context.getConfig().kafkaPartitionFallback;
        this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey, partitionColumns, partitionFallback);
        this.ddlPartitioner = new MaxwellKafkaPartitioner(hash, "database", null,"database");
        this.ddlTopic =  context.getConfig().ddlKafkaTopic;

        if ( context.getConfig().kafkaKeyFormat.equals("hash") )
            keyFormat = RowMap.KeyFormat.HASH;
        else
            keyFormat = RowMap.KeyFormat.ARRAY;

        this.inflightMessages = new InflightMessageList();
    }

    abstract protected Integer getNumPartitions(String topic);

    protected String generateTopic(String topic, RowMap r){
        return topic.replaceAll("%\\{database\\}", r.getDatabase()).replaceAll("%\\{table\\}", r.getTable());
    }

    protected void skipMessage(RowMap r) {
        inflightMessages.addMessage(r.getPosition());
        BinlogPosition newPosition = inflightMessages.completeMessage(r.getPosition());

        if ( newPosition != null ) {
            context.setPosition(newPosition);
        }
    }
}
