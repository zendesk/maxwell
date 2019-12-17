package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowIdentity;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.util.HashMap;
import java.util.Map;

public class MaxwellRedisProducer extends AbstractProducer implements StoppableTask {
	private static final Logger logger = LoggerFactory.getLogger(MaxwellRedisProducer.class);
	private final String channel;
	private final boolean interpolateChannel;
	private final String redisType;
	private final Jedis jedis;

	@Deprecated
	public MaxwellRedisProducer(MaxwellContext context, String redisPubChannel, String redisListKey, String redisType) {
		this(context);
	}

	public MaxwellRedisProducer(MaxwellContext context) {
		super(context);

		this.channel = context.getConfig().redisKey;
		this.interpolateChannel = channel.contains("%{");
		this.redisType = context.getConfig().redisType;

		jedis = new Jedis(context.getConfig().redisHost, context.getConfig().redisPort);
		jedis.connect();

		if (context.getConfig().redisAuth != null) {
			jedis.auth(context.getConfig().redisAuth);
		}

		if (context.getConfig().redisDatabase > 0) {
			jedis.select(context.getConfig().redisDatabase);
		}
	}

	private String generateChannel(RowIdentity pk){
		if (interpolateChannel) {
			return channel.replaceAll("%\\{database}", pk.getDatabase()).replaceAll("%\\{table}", pk.getTable());
		}

		return channel;
	}

	private void sendToRedis(RowMap msg) throws Exception {
		String messageStr = msg.toJSON(outputConfig);

		String channel = this.generateChannel(msg.getRowIdentity());

		switch (redisType) {
			case "lpush":
				jedis.lpush(channel, messageStr);
				break;
			case "rpush":
				jedis.rpush(this.channel, messageStr);
				break;
			case "xadd":
				Map<String, String> message = new HashMap<>();

				String jsonKey = this.context.getConfig().redisStreamJsonKey;

				if (jsonKey == null) {
					// TODO dot notated map impl in RowMap.toJson
					throw new IllegalArgumentException("Stream requires key name for serialized JSON value");
				}
				else {
					message.put(jsonKey, messageStr);
				}

				// TODO timestamp resolution coercion
				// 			Seconds or milliseconds, never mixing precision
				//      	DML events will natively emit millisecond precision timestamps
				//      	CDC events will natively emit second precision timestamp
				// TODO configuration option for if we want the msg timestamp to become the message ID
				//			Requires completion of previous TODO
				jedis.xadd(channel, StreamEntryID.NEW_ENTRY, message);
				break;
			case "pubsub":
			default:
				jedis.publish(channel, messageStr);
				break;
		}

		if (logger.isDebugEnabled()) {
			switch (redisType) {
				case "lpush":
					logger.debug("->  queue (left):" + channel + ", msg:" + msg);
					break;
				case "rpush":
					logger.debug("->  queue (right):" + channel + ", msg:" + msg);
					break;
				case "xadd":
					logger.debug("->  stream:" + channel + ", msg:" + msg);
					break;
				case "pubsub":
				default:
					logger.debug("->  channel:" + channel + ", msg:" + msg);
					break;
			}
		}

		this.succeededMessageCount.inc();
		this.succeededMessageMeter.mark();
	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getNextPosition());
			return;
		}

		for (int cxErrors = 0; cxErrors < 2; cxErrors++) {
			try {
				this.sendToRedis(r);
				break;
			} catch (Exception e) {
				if (e instanceof JedisConnectionException) {
					logger.warn("lost connection to server, trying to reconnect...", e);
					jedis.disconnect();
					jedis.connect();
				} else {
					this.failedMessageCount.inc();
					this.failedMessageMeter.mark();
					logger.error("Exception during put", e);

					if (!context.getConfig().ignoreProducerError) {
						throw new RuntimeException(e);
					}
				}
			}
		}

		if (r.isTXCommit()) {
			context.setPosition(r.getNextPosition());
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
