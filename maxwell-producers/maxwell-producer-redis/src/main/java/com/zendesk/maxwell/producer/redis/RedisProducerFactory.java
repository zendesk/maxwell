package com.zendesk.maxwell.producer.redis;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;

public class RedisProducerFactory implements ProducerFactory {
    @Override
    public Producer createProducer(MaxwellContext context) {
        RedisProducerConfiguration configuration = (RedisProducerConfiguration)context.getConfig().getProducerConfiguration();
        return new MaxwellRedisProducer(context, configuration);
    }
}
