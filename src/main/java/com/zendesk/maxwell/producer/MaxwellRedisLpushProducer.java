package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class MaxwellRedisLpushProducer extends AbstractProducer implements StoppableTask {
        private static final Logger logger = LoggerFactory.getLogger(MaxwellRedisLpushProducer.class);
        private final String listkey;
	private final Jedis jedis;

        public MaxwellRedisLpushProducer(MaxwellContext context, String redisListKey) {
                super(context);

		listkey = redisListKey;

                jedis = new Jedis(context.getConfig().redisHost, context.getConfig().redisPort);
                jedis.connect();
                if (context.getConfig().redisAuth != null) {
                        jedis.auth(context.getConfig().redisAuth);
                }
                if (context.getConfig().redisDatabase > 0) {
                        jedis.select(context.getConfig().redisDatabase);
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
                        jedis.lpush(this.listkey, msg);
                        this.succeededMessageCount.inc();
                        this.succeededMessageMeter.mark();
                } catch (Exception e) {
                        this.failedMessageCount.inc();
                        this.failedMessageMeter.mark();
                        logger.error("Exception during put", e);

                        if (!context.getConfig().ignoreProducerError) {
                                throw new RuntimeException(e);
                        }
                }

                if ( r.isTXCommit() ) {
                        context.setPosition(r.getPosition());
                }

                if ( logger.isDebugEnabled()) {
                        logger.debug("->  queue:" + listkey + ", msg:" + msg);
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
