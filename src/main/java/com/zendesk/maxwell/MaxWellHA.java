package com.zendesk.maxwell;

import com.zendesk.maxwell.util.Logging;
import com.zendesk.maxwell.zk.ZkPropKeys;
import com.zendesk.maxwell.zk.ZkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class MaxWellHA {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxWellHA.class);

	public static void main(String[] args) throws Exception {
		Logging.setupLogBridging();
		final MaxwellConfig config = new MaxwellConfig(args);

		if (config.log_level != null)
			Logging.setLevel(config.log_level);

		final ZkService zkService = new ZkService(config.customProducerProperties.getProperty(ZkPropKeys.zkServers));

		while (true) {
			final boolean isMaster = zkService.isMaster();
			LOGGER.debug("isMaster:" + isMaster);
			if (isMaster) {
				CompletableFuture.runAsync(() -> createMaxWellAndStart(config)).get();
			}
			TimeUnit.SECONDS.sleep(3);
		}
	}

	private static void createMaxWellAndStart(MaxwellConfig config) {
		Maxwell maxwell = null;
		try {
			maxwell = new Maxwell(config);
			maxwell.start();
			LOGGER.info("MaxWell instance finished");
		} catch (Exception e) {
			LOGGER.error("createMaxWellAndStart error", e);
		} finally {
			if (maxwell != null) {
				try {
					maxwell.terminate();
				} catch (Exception e) {
					LOGGER.error("maxwell.terminate error", e);
				}
			}
		}

	}
}
