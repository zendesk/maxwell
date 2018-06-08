package com.zendesk.maxwell.producer.kafka;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import com.zendesk.maxwell.api.row.RowMapFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerFactory implements ProducerFactory {
    private final RowMapFactory rowMapFactory;

    @Autowired
    public KafkaProducerFactory(RowMapFactory rowMapFactory) {
        this.rowMapFactory = rowMapFactory;
    }

    @Override
    public Producer createProducer(MaxwellContext context) {
        KafkaProducerConfiguration configuration = (KafkaProducerConfiguration)context.getConfig().getProducerConfiguration();
        return new MaxwellKafkaProducer(context, configuration, rowMapFactory);
    }
}
