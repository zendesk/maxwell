package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.RowMap.KeyFormat;
import com.zendesk.maxwell.producer.partitioners.MaxwellKafkaPartitioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;

class KafkaCallback implements Callback {
	public static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private InflightMessageList inflightMessages;
	private final MaxwellContext context;
	private final BinlogPosition position;
	private final boolean isTXCommit;
	private final String json;
	private final String key;

	public KafkaCallback(InflightMessageList inflightMessages, BinlogPosition position, boolean isTXCommit, MaxwellContext c, String key, String json) {
		this.inflightMessages = inflightMessages;
		this.context = c;
		this.position = position;
		this.isTXCommit = isTXCommit;
		this.key = key;
		this.json = json;
	}

	@Override
	public void onCompletion(RecordMetadata md, Exception e) {

		if ( e != null ) {
			if ( e instanceof RecordTooLargeException ) {
				LOGGER.error("RecordTooLargeException @ " + position + " -- " + key);
				LOGGER.error(e.getLocalizedMessage());
				LOGGER.error("Considering raising max.request.size broker-side.");

				markCompleted();
			} else {
				throw new RuntimeException(e);
			}
		} else {
			if ( LOGGER.isDebugEnabled()) {
				LOGGER.debug("->  key:" + key + ", partition:" +md.partition() + ", offset:" + md.offset());
				LOGGER.debug("   " + this.json);
				LOGGER.debug("   " + position);
				LOGGER.debug("");
			}

			markCompleted();
		}
	}

	private void markCompleted() {
		if ( isTXCommit ) {
			BinlogPosition newPosition = inflightMessages.completeMessage(position);

			if ( newPosition != null ) {
				try {
					context.setPosition(newPosition);
				} catch ( SQLException e ) {
					e.printStackTrace();
				}
			}
		}
	}

}

public class MaxwellKafkaProducer extends AbstractProducer {
	static final Object KAFKA_DEFAULTS[] = {
		"compression.type", "gzip",
		"metadata.fetch.timeout.ms", 5000,
		"retries", 1
	};
	private final InflightMessageList inflightMessages;
	private final KafkaProducer<String, String> kafka;
	private String topic;
	private final int numPartitions;
	private final MaxwellKafkaPartitioner partitioner;
	private final KeyFormat keyFormat;
    private boolean topicPerDbtable;
    private boolean topicPerDb;
	public MaxwellKafkaProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic ,boolean topicPerDbtable ,boolean topicPerDb) {
		super(context);

		this.topic = kafkaTopic;
		if ( this.topic == null ) {
			this.topic = "maxwell";
		}
        this.topicPerDb=topicPerDb;
        this.topicPerDbtable=topicPerDbtable;

		this.setDefaults(kafkaProperties);
		this.kafka = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());

		this.numPartitions = kafka.partitionsFor(topic).size(); //returns 1 for new topics

		String hash = context.getConfig().kafkaPartitionHash;
		String partitionKey = context.getConfig().kafkaPartitionKey;
		this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey);

		if ( context.getConfig().kafkaKeyFormat.equals("hash") )
			keyFormat = KeyFormat.HASH;
		else
			keyFormat = KeyFormat.ARRAY;

		this.inflightMessages = new InflightMessageList();
	}

        /* if topic is database then set separate topic for each database
     * if topic is database.table then set separate topic for each database.table
     * enable auto.create.topics.enable=true in server.properties*/

    public String getDbTopic(String database,String table,String topic){
        StringBuilder topicValue = new StringBuilder();
          if (topicPerDbtable){
            topicValue.append(database);
            topicValue.append(".");
            topicValue.append(table);
            return  topicValue.toString();
        }
        else if(topicPerDb) {
              topicValue.append(database);
              return  topicValue.toString();
          }
        return topic;
    }

	@Override
	public void push(RowMap r) throws Exception {
		String key = r.pkToJson(keyFormat);
		String value = r.toJSON();
        String dbTopic =getDbTopic(r.getDatabase(),r.getTable(),topic) ; // get kafka topic if multi topic enable
		ProducerRecord<String, String> record =
				new ProducerRecord<>(dbTopic, this.partitioner.kafkaPartition(r, this.numPartitions), key, value);

		if ( r.isTXCommit() )
			inflightMessages.addMessage(r.getPosition());


		/* if debug logging isn't enabled, release the reference to `value`, which can ease memory pressure somewhat */
		if ( !KafkaCallback.LOGGER.isDebugEnabled() )
			value = null;

		KafkaCallback callback = new KafkaCallback(inflightMessages, r.getPosition(), r.isTXCommit(), this.context, key, value);

		kafka.send(record, callback);

	}

	@Override
	public void writePosition(BinlogPosition p) throws SQLException {
		// ensure that we don't prematurely advance the binlog pointer.
		inflightMessages.addMessage(p);
		inflightMessages.completeMessage(p);
	}

	private void setDefaults(Properties p) {
		for(int i=0 ; i < KAFKA_DEFAULTS.length; i += 2) {
			String key = (String) KAFKA_DEFAULTS[i];
			Object val = KAFKA_DEFAULTS[i + 1];

			if ( !p.containsKey(key)) {
				p.put(key, val);
			}
		}
	}
}
