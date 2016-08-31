package com.zendesk.maxwell.distributed;

/**
 * Created by springloops on 2016. 8. 31..
 */

import com.zendesk.maxwell.MaxwellContext;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class ActiveMaxwellLockFactory extends StateModelFactory<ActiveMaxwellLock> {

  private final MaxwellContext context;
  private ActiveMaxwellLock activeMaxwellLock;

  public ActiveMaxwellLockFactory(MaxwellContext context) {
      this.context = context;
  }

  @Override
  public ActiveMaxwellLock createNewStateModel(String resourceName, String lockName) {
    activeMaxwellLock = new ActiveMaxwellLock((HAMaxwellConfig) this.context.getConfig());
    return activeMaxwellLock;
  }

  protected ActiveMaxwellLock getActiveMaxwellLock() {
      ActiveMaxwellLock lock = activeMaxwellLock;
      return lock;
  }

}
