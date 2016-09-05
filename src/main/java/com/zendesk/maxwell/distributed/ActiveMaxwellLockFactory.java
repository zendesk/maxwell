package com.zendesk.maxwell.distributed;

/**
 * Created by springloops on 2016. 8. 31..
 */

import com.zendesk.maxwell.MaxwellContext;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class ActiveMaxwellLockFactory extends StateModelFactory<ActiveMaxwellLock> {

    private final MaxwellContext context;
    private final MaxwellHAExceptionHandler handler;

    private ActiveMaxwellLock activeMaxwellLock;

    protected ActiveMaxwellLockFactory(MaxwellContext context) {
        this(context, new MaxwellHAExceptionDefaultHandler());
    }

    protected ActiveMaxwellLockFactory(MaxwellContext context, MaxwellHAExceptionHandler handler) {
        this.context = context;
        this.handler = handler;
    }

    @Override
    public ActiveMaxwellLock createNewStateModel(String resourceName, String lockName) {
        activeMaxwellLock = new ActiveMaxwellLock(this.context.getConfig(), handler);
        return activeMaxwellLock;
    }

    protected ActiveMaxwellLock getActiveMaxwellLock() {
        final ActiveMaxwellLock lock = activeMaxwellLock;
        return lock;
    }

}
