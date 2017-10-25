package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.util.StoppableTask;
import redis.clients.jedis.Jedis;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxwellRedisProducer extends AbstractProducer implements StoppableTask {
	private static final Logger logger = LoggerFactory.getLogger(MaxwellRedisProducer.class);
	private final String channel;
	private final Jedis jedis;


	public MaxwellRedisProducer(MaxwellContext context, String redisPubChannel) {
		super(context);

		channel = redisPubChannel;

		jedis = new Jedis(context.getConfig().redisHost, context.getConfig().redisPort);
		jedis.connect();
		if (context.getConfig().redisAuth != null) {
			jedis.auth(context.getConfig().redisAuth);
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getPosition());
			return;
		}

		String msg = r.toJSON(outputConfig);

		jedis.publish(this.channel, msg);

		if ( r.isTXCommit() ) {
			context.setPosition(r.getPosition());
		}

		if ( logger.isDebugEnabled()) {
			logger.debug("->  channel:" + channel + ", msg:" + msg);
		}
	}

	@Override
	public void requestStop() {
		jedis.close();
	}

	@Override
	public void awaitStop(Long timeout) { }

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}
}
