package com.zendesk.maxwell.core.producer.impl.redis;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

@Service
public class RedisProducerFactory implements NamedProducerFactory {
	@Override
	public String getName() {
		return "redis";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		return new MaxwellRedisProducer(context, context.getConfig().redisPubChannel, context.getConfig().redisListKey, context.getConfig().redisType);
	}
}
