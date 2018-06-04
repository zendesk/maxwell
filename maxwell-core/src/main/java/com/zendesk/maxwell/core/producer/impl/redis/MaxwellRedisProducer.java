package com.zendesk.maxwell.core.producer.impl.redis;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.util.StoppableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class MaxwellRedisProducer extends AbstractProducer implements StoppableTask {
	private static final Logger logger = LoggerFactory.getLogger(MaxwellRedisProducer.class);
	private final String channel;
	private final String listkey;
	private final String redistype;
	private final Jedis jedis;

	public MaxwellRedisProducer(MaxwellContext context) {
		super(context);

		RedisProducerConfiguration config = context.getConfig().getProducerConfigOrThrowExceptionWhenNotDefined();

		channel = config.getRedisPubChannel();
		listkey = config.getRedisListKey();
		redistype = config.getRedisType();

		jedis = new Jedis(config.getRedisHost(), config.getRedisPort());
		jedis.connect();
		if (config.getRedisAuth() != null) {
			jedis.auth(config.getRedisAuth());
		}
		if (config.getRedisDatabase() > 0) {
			jedis.select(config.getRedisDatabase());
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getPosition());
			return;
		}

		String msg = r.toJSON(outputConfig);
		try {
			switch (redistype){
				case "lpush":
					jedis.lpush(this.listkey, msg);
					break;
				case "pubsub":
				default:
					jedis.publish(this.channel, msg);
					break;
			}
			this.succeededMessageCount.inc();
			this.succeededMessageMeter.mark();
		} catch (Exception e) {
			this.failedMessageCount.inc();
			this.failedMessageMeter.mark();
			logger.error("Exception during put", e);

			if (!context.getConfig().isIgnoreProducerError()) {
				throw new RuntimeException(e);
			}
		}

		if ( r.isTXCommit() ) {
			context.setPosition(r.getPosition());
		}

		if ( logger.isDebugEnabled()) {
			switch (redistype){
				case "lpush":
					logger.debug("->  queue:" + listkey + ", msg:" + msg);
					break;
				case "pubsub":
				default:
					logger.debug("->  channel:" + channel + ", msg:" + msg);
					break;
			}
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
