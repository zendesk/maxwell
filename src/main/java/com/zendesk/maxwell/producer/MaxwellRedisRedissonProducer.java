package com.zendesk.maxwell.producer;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowIdentity;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;

public class MaxwellRedisRedissonProducer extends AbstractProducer implements StoppableTask {

    private static final Logger logger = LoggerFactory.getLogger(MaxwellRedisRedissonProducer.class);

	private final String channel;
	private final boolean interpolateChannel;
    private final String redisType;
    
    private RedissonClient redissonClient;

    public MaxwellRedisRedissonProducer(MaxwellContext context) {
        super(context);

		this.channel = context.getConfig().redisKey;
		this.interpolateChannel = channel.contains("%{");
        this.redisType = context.getConfig().redisType;
        
        //create redisson instance
        Config config = new Config();
        config.useSingleServer()
                .setAddress(String.format("redis://%s:%s", context.getConfig().redisHost, context.getConfig().redisPort))
                .setClientName("maxwell-redis-client")
                .setSubscriptionConnectionMinimumIdleSize(0)
                .setConnectionMinimumIdleSize(2)
                .setConnectionPoolSize(2);
        config.setCodec(new StringCodec());

        redissonClient = Redisson.create(config);
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
                redissonClient.getDeque(channel).addFirst(messageStr);
                break;
            case "rpush":
                redissonClient.getList(channel).add(messageStr);
                break;
            case "xadd":

                String jsonKey = this.context.getConfig().redisStreamJsonKey;

                if (jsonKey == null) {
                    // TODO dot notated map impl in RowMap.toJson
                    throw new IllegalArgumentException("Stream requires key name for serialized JSON value");
                }

                // TODO timestamp resolution coercion
                // 			Seconds or milliseconds, never mixing precision
                //      	DML events will natively emit millisecond precision timestamps
                //      	CDC events will natively emit second precision timestamp
                // TODO configuration option for if we want the msg timestamp to become the message ID
                //			Requires completion of previous TODO

                redissonClient.getStream(channel).add(jsonKey, messageStr);
                break;
            case "pubsub":
            default:
                redissonClient.getTopic(channel).publish(messageStr);
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
				if (e instanceof RedisException) {
					logger.warn("Error with redis communication, will try again", e);
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
    public void requestStop() throws Exception {
        redissonClient.shutdown();
    }

	@Override
	public void awaitStop(Long timeout) { }

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}
    
}