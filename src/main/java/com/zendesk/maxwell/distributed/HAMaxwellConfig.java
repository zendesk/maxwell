package com.zendesk.maxwell.distributed;

import com.zendesk.maxwell.MaxwellConfig;
import joptsimple.OptionParser;

/**
 * Created by springloops on 2016. 8. 31..
 */
public class HAMaxwellConfig extends MaxwellConfig {

    public HAMaxwellConfig(String[] args) {
        super(args);
        buildOptionParser();
    }

    @Override
    protected OptionParser buildOptionParser() {
        // TODO: 2016. 8. 31. Do Parse config for Active Standby Maxwell.
        // TODO: 2016. 8. 31. Please Look MaxwellConfig in the maxwell_hamode branch.
    }
}
