package com.zendesk.maxwell.distributed;

/**
 * Created by springloops on 2016. 8. 31..
 */

import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellConfig;
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

    private final MaxwellConfig config;
    private final MaxwellHAExceptionHandler handler;

    private Maxwell maxwell;

    protected ActiveMaxwellLock(MaxwellConfig config, MaxwellHAExceptionHandler handler) {
        this.config = config;
        this.handler = handler;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void lock(Message m, NotificationContext context) throws Exception {
        LOGGER.info(context.getManager().getClusterName() + " : " + context.getManager().getInstanceName() + " applied Active");
        LOGGER.info("Start Maxwell Active Node");
        maxwell = new Maxwell(this.config);
        try {
            maxwell.run();
        } catch (Exception e) {
            e.printStackTrace();
            handler.exceptionHandling(maxwell, e);
        }

    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void release(Message m, NotificationContext context) {
        LOGGER.info("Maxwell Active Node changed status");
        maxwell.terminate();
    }

    protected Maxwell getMaxwell() {
        return maxwell;
    }

}
