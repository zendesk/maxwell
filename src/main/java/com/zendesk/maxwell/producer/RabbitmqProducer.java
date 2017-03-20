package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.MessageBusManager;





public class RabbitmqProducer extends AbstractProducer {
	
  static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqProducer.class);
  private MessageBusManager _messageBusManager = MessageBusManager.getInstance();
  
  public RabbitmqProducer(MaxwellContext context) {

      super(context);
      LOGGER.info("inside maxwell rabbitmq producer");
      MaxwellConfig config = context.getConfig();
      _messageBusManager.start(config.rabbitmqHost, Integer.valueOf(config.rabbitmqPort), config.rabbitmqUser, config.rabbitmqPassword);

  }

  @Override
  public void push(RowMap r) throws Exception {
      String value = r.toJSON(outputConfig);
      if (value == null) {
          return;
      }
      String routingKey = r.getTable();

      LOGGER.info("Thread-id: "+Thread.currentThread().getId()+": new event is coming: "+value);
      
      String dbName=r.getDatabase();
      
            
      _messageBusManager.sendNotificationToTopicExchangeForSpecifiedDbName(dbName, routingKey, value, Thread.currentThread().getId(), context.getConfig().rabbitmqExchangeType);

      if ( r.isTXCommit() ) {
          context.setPosition(r.getPosition());

      }
      if ( LOGGER.isDebugEnabled()) {
          LOGGER.debug("->  routing key:" + routingKey + ", partition:" + value);
      }
  }
  
  
  @Override
	public void stop() throws IOException, TimeoutException {
		super.stop();
		this._messageBusManager.stop();
	}
  
}	
	