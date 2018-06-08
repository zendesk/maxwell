package com.zendesk.maxwell.producer.rabbitmq;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;

public class RabbitmqProducerFactory implements ProducerFactory {
    @Override
    public Producer createProducer(MaxwellContext context) {
        RabbitmqProducerConfiguration configuration = (RabbitmqProducerConfiguration)context.getConfig().getProducerConfiguration();
        return new RabbitmqProducer(context, configuration);
    }
}
