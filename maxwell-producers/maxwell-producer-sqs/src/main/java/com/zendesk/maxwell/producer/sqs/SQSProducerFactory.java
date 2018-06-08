package com.zendesk.maxwell.producer.sqs;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;

public class SQSProducerFactory implements ProducerFactory {
    @Override
    public Producer createProducer(MaxwellContext context) {
        SQSProducerConfiguration configuration = (SQSProducerConfiguration)context.getConfig().getProducerConfiguration();
        return new MaxwellSQSProducer(context, configuration.getSqsQueueUri());
    }
}
