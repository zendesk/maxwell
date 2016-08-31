package com.zendesk.maxwell.distributed;

/**
 * Created by springloops on 2016. 8. 31..
 */
import com.zendesk.maxwell.Maxwell;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(initialState = "OFFLINE", states = {
    "OFFLINE", "ONLINE"
})
public class ActiveMaxwellLock extends StateModel {
  static final Logger LOGGER = LoggerFactory.getLogger(ActiveMaxwellLock.class);

  private final HAMaxwellConfig config;
  private Maxwell maxwell;

  public ActiveMaxwellLock(HAMaxwellConfig config) {
      this.config = config;
  }

  @Transition(from = "OFFLINE", to = "ONLINE")
  public void lock(Message m, NotificationContext context) throws Exception {
    LOGGER.info(context.getManager().getClusterName() + " : " + context.getManager().getInstanceName() + " applied Active");
    LOGGER.info("Start Maxwell Active Node");
    maxwell = new Maxwell(this.config);
    maxwell.run();
  }

  @Transition(from = "ONLINE", to = "OFFLINE")
  public void release(Message m, NotificationContext context) {
    LOGGER.info("Maxwell Active Node changed status");
    maxwell.terminate();
  }

}
