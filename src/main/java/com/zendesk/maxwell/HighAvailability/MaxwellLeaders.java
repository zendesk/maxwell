package com.zendesk.maxwell.HighAvailability;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.util.CuratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MaxwellLeaders {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellLeaders.class);

	public static void main(String[] args) {
		MaxwellConfig config = new MaxwellConfig(args);
		if(config.haMode){
			CuratorUtils cu = new CuratorUtils();
			cu.setZookeeperServer(config.zookeeperServer);
			cu.setSessionTimeoutMs(config.sessionTimeoutMs);
			cu.setConnectionTimeoutMs(config.connectionTimeoutMs);
			cu.setMaxRetries(config.maxRetries);
			cu.setBaseSleepTimeMs(config.baseSleepTimeMs);
			cu.init();
			List<String> instances = cu.getInstances();
			if(0 == instances.size()){
				LOGGER.info("Maxwell is not a high availability mode Or maxwell is not started");
			}else {
				LOGGER.info("Maxwell's leaders now are -> " + instances.get(0));
			}
		}else {
			LOGGER.info("Maxwell is not a high availability mode");
		}
	}

}
