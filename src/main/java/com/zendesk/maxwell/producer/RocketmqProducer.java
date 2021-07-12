package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketmqProducer extends AbstractProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(RocketmqProducer.class);
	// 实例化消息生产者Producer
	private DefaultMQProducer producer;

	private String rocketmqSendTopic;
	private String rocketmqTags;


	public RocketmqProducer(MaxwellContext context) {
		super(context);
		try {
			producer = new DefaultMQProducer(context.getConfig().rocketmqProducerGroup);
			// 设置NameServer的地址
			producer.setNamesrvAddr(context.getConfig().rocketmqNamesrvAddr);

			rocketmqSendTopic = context.getConfig().rocketmqSendTopic;
			rocketmqTags = context.getConfig().rocketmqTags;
			// 启动Producer实例
			producer.start();
		} catch (MQClientException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getNextPosition());
			return;
		}
		String value = r.toJSON(outputConfig);
		// 创建消息，并指定Topic，Tag和消息体
		Message msg = new Message(rocketmqSendTopic /* Topic */,
				rocketmqTags /* Tag */,
				(value).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
		);

		producer.send(msg);

		if ( r.isTXCommit() ) {
			context.setPosition(r.getNextPosition());
		}
		if ( LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  topic:" + rocketmqSendTopic + ", tags:" + rocketmqTags);
		}
	}


}
