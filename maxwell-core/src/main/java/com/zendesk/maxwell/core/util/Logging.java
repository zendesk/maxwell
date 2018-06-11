package com.zendesk.maxwell.core.util;

import com.zendesk.maxwell.api.config.ConfigurationSupport;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class Logging {
	private final ConfigurationSupport configurationSupport;

	@Autowired
	public Logging(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	public void configureLevelFrom(final Properties configurationOptions){
		String logLevel = configurationSupport.fetchOption("log_level", configurationOptions, null);
		if(logLevel != null){
			setLevel(logLevel);
		}
	}

	public void setLevel(String level) {
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
		loggerConfig.setLevel(Level.valueOf(level));
		ctx.updateLoggers();  // This causes all Loggers to refetch information from their LoggerConfig.
	}

	public static void setupLogBridging() {
		// Optionally remove existing handlers attached to j.u.l root logger
		SLF4JBridgeHandler.removeHandlersForRootLogger();  // (since SLF4J 1.6.5)

		// add SLF4JBridgeHandler to j.u.l's root logger, should be done once during
		// the initialization phase of your application
		SLF4JBridgeHandler.install();
	}
}
