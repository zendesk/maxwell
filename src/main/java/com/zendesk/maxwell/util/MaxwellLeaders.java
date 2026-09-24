package com.zendesk.maxwell.util;

import com.zendesk.maxwell.MaxwellConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * Get maxwell highly available leaders (maxwell on zk)
 */
public class MaxwellLeaders {

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellLeaders.class);

	public static void main(String[] args) {

		Logging.setupLogBridging();
		MaxwellConfig config = new MaxwellConfig(args);

		if ( config.log_level != null ) {
			Logging.setLevel(config.log_level);
		}else {
			Logging.setLevel("INFO");
		}

		if( "zookeeper".equals(config.haMode)){
			CuratorUtils cu = new CuratorUtils();
			cu.setZookeeperServer(config.zookeeperServer);
			cu.setSessionTimeoutMs(config.zookeeperSessionTimeoutMs);
			cu.setConnectionTimeoutMs(config.zookeeperConnectionTimeoutMs);
			cu.setMaxRetries(config.zookeeperMaxRetries);
			cu.setBaseSleepTimeMs(config.zookeeperRetryWaitMs);
			cu.setClientId(config.clientID);
			String electPath = "/" + config.clientID + "/services";
			String masterPath = "/" + config.clientID + "/leader";
			cu.setElectPath(electPath);
			cu.setMasterPath(masterPath);
			cu.init();
			List<String> instances = null;
			try {
				instances = cu.getInstances();
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.error("The path does not exist or is empty. Please check whether the clientID is correct. clientID = " + config.clientID);
				System.exit(1);
			}

			if(0 == instances.size()){
				LOGGER.info("Maxwell is not a high availability mode Or maxwell is not started");
			}else {
				LOGGER.info("clientID:"+config.clientID + ":leaders now are -> {}",instances.get(0));
			}
		}else {
			LOGGER.error("make sure ha = 'zookeeper'. ha = " + config.haMode);
		}
	}
}
