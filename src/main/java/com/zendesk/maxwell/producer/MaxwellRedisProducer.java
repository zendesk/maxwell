package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowIdentity;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolAbstract;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MaxwellRedisProducer extends AbstractProducer implements StoppableTask {
	private static final Logger logger = LoggerFactory.getLogger(MaxwellRedisProducer.class);
	private final String channel;
	private final boolean interpolateChannel;
	private final String redisType;

	private static JedisPoolAbstract jedisPool;

	@Deprecated
	public MaxwellRedisProducer(MaxwellContext context, String redisPubChannel, String redisListKey, String redisType) {
		this(context);
	}

	public MaxwellRedisProducer(MaxwellContext context) {
		super(context);

		this.channel = context.getConfig().redisKey;
		this.interpolateChannel = channel.contains("%{");
		this.redisType = context.getConfig().redisType;

		String redisSentinelName = context.getConfig().redisSentinelMasterName;
		if (redisSentinelName != null) {
			jedisPool = new JedisSentinelPool(
				context.getConfig().redisSentinelMasterName,
				getRedisSentinels(context.getConfig().redisSentinels),
				createRedisPoolConfig(),
				Protocol.DEFAULT_TIMEOUT,
				context.getConfig().redisAuth, //even if not present jedispool will handle a null value
				context.getConfig().redisDatabase); //even if not present jedispool will handle a null value
		} else {
			jedisPool = new JedisPool(
				createRedisPoolConfig(),
				context.getConfig().redisHost,
				context.getConfig().redisPort,
				Protocol.DEFAULT_TIMEOUT,
				context.getConfig().redisAuth, //even if not present jedispool will handle a null value
				context.getConfig().redisDatabase); //even if not present jedispool will handle a null value
		}
	}

	private Set<String> getRedisSentinels(String redisSentinels) {
		return new HashSet<>(Arrays.asList(redisSentinels.split(",")));
	}

	private JedisPoolConfig createRedisPoolConfig() {
		
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		//2 is the most we'll need, one for the bootstrap task and another to the main maxwell thread
		poolConfig.setMaxTotal(2); 
		poolConfig.setMaxIdle(2);
		poolConfig.setMinIdle(0);
		poolConfig.setTestOnBorrow(true);
		poolConfig.setBlockWhenExhausted(true);
		return poolConfig;
	}

	private String generateChannel(RowIdentity pk){
		if (interpolateChannel) {
			return channel.replaceAll("%\\{database}", pk.getDatabase()).replaceAll("%\\{table}", pk.getTable());
		}

		return channel;
	}

	private Jedis getJedisResource() {
		return jedisPool.getResource();
	}

	private void sendToRedis(RowMap msg) throws Exception {

		String messageStr = msg.toJSON(outputConfig);
		String channel = this.generateChannel(msg.getRowIdentity());

		try (Jedis jedis = this.getJedisResource()) {

			switch (redisType) {
				case "lpush":
					jedis.lpush(channel, messageStr);
					break;
				case "rpush":
					jedis.rpush(channel, messageStr);
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
	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getNextPosition());
			return;
		}

		boolean sentToRedis = false;

		for (int cxErrors = 0; cxErrors < 2; cxErrors++) {
			try {
				this.sendToRedis(r);
				sentToRedis = true;
				break;
			} catch (Exception e) {
				if (e instanceof JedisConnectionException) {
					logger.warn("lost connection to server, will try again with another connection from pool", e);
				} else {

					logger.error("Exception during put", e);

					if (!context.getConfig().ignoreProducerError) {
						throw new RuntimeException(e);
					}
				}
			}
		}

		if (sentToRedis) {
			this.succeededMessageCount.inc();
			this.succeededMessageMeter.mark();
		} else {
			this.failedMessageCount.inc();
			this.failedMessageMeter.mark();
		}

		if (r.isTXCommit()) {
			context.setPosition(r.getNextPosition());
		}
	}

	@Override
	public void requestStop() {
		jedisPool.close();
	}

	@Override
	public void awaitStop(Long timeout) { }

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}
}
