package com.zendesk.maxwell.producer.pubsub;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import com.zendesk.maxwell.api.producer.ProducerInstantiationException;

import java.io.IOException;

public class PubSubProducerFactory implements ProducerFactory {
    @Override
    public Producer createProducer(MaxwellContext context) {
        try {
            PubsubProducerConfiguration configuration = (PubsubProducerConfiguration)context.getConfig().getProducerConfiguration();
            return new MaxwellPubsubProducer(context, configuration.getPubsubProjectId(), configuration.getPubsubTopic(), configuration.getDdlPubsubTopic());
        } catch (IOException e) {
            throw new ProducerInstantiationException(e);
        }
    }
}
