package com.zendesk.maxwell.producer.kinesis;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;

public class KinesisProducerFactory implements ProducerFactory {
    @Override
    public Producer createProducer(MaxwellContext context) {
        KinesisProducerConfiguration configuration = (KinesisProducerConfiguration)context.getConfig().getProducerConfiguration();
        return new MaxwellKinesisProducer(context, configuration);
    }
}
