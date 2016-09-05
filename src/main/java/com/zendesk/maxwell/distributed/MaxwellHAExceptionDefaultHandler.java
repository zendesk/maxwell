package com.zendesk.maxwell.distributed;

import com.zendesk.maxwell.Maxwell;

/**
 * Created by springloops on 2016. 9. 5..
 */
public class MaxwellHAExceptionDefaultHandler implements MaxwellHAExceptionHandler {
    @Override
    public void exceptionHandling(Maxwell maxwell, Exception exception) {
        maxwell.terminate();
        System.exit(1);
    }
}
