package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.util.Properties;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

class KafkaCallback implements Callback {
	private final MaxwellConfig config;
	private final MaxwellAbstractRowsEvent event;

	public KafkaCallback(MaxwellAbstractRowsEvent e, MaxwellConfig c) {
		this.config = c;
		this.event = e;
	}

	@Override
	public void onCompletion(RecordMetadata md, Exception e) {
		if ( e != null ) {
            e.printStackTrace();
		} else {
            System.out.println("The offset of the record we just sent is: " + md.offset());
            try {
            	config.setInitialPosition(new BinlogPosition(event.getHeader().getNextPosition(), event.getBinlogFilename()));
            } catch (IOException e1) {
            	// TODO Auto-generated catch block
            	e1.printStackTrace();
            }
		}
	}

}
public class MaxwellKafkaProducer extends AbstractProducer {
	private final KafkaProducer<byte[], byte[]> kafka;

	public MaxwellKafkaProducer(MaxwellConfig config, Properties kafkaProperties) {
		super(config);

		this.kafka = new KafkaProducer<>(kafkaProperties, new ByteArraySerializer(), new ByteArraySerializer());
	}

	private String kafkaTopic(MaxwellAbstractRowsEvent e) {
		return e.getTable().getDatabase().getName();
	}

	@Override
	public void push(MaxwellAbstractRowsEvent e) throws Exception {
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(kafkaTopic(e), e.toJSON().getBytes());

		kafka.send(record, new KafkaCallback(e, this.config));
	}

}
