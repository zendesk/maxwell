package com.zendesk.maxwell.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import joptsimple.*;

import com.zendesk.maxwell.MaxwellMysqlConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfig.class);
	static final protected String DEFAULT_CONFIG_FILE = "config.properties";
	protected abstract OptionParser buildOptionParser();

	protected void usage(String banner, MaxwellOptionParser optionParser, String section) {
		System.out.println(banner);
		System.out.println();
		try {
			optionParser.printHelpOn(System.out, section);
			System.exit(1);
		} catch (IOException e) { }
	}

	protected void usage(String string) {
		System.out.println(string);
		System.out.println();
		try {
			buildOptionParser().printHelpOn(System.out);
			System.exit(1);
		} catch (IOException e) { }
	}

	protected void usageForOptions(String string, final String... filterOptions) {
		BuiltinHelpFormatter filteredHelpFormatter = new BuiltinHelpFormatter(200, 4) {
			@Override
			public String format(Map<String, ? extends OptionDescriptor> options) {
				this.addRows(options.values());
				String output = this.formattedHelpOutput();
				String[] lines = output.split("\n");

				String filtered = "";
				int i = 0;
				for ( String l : lines ) {
					boolean showLine = false;

					if ( l.contains("--help") || i++ < 2 ) // take the first 3 lines, these are the header
						showLine = true;
					for ( String o : filterOptions )  {
						if ( l.contains(o) && !l.startsWith("--__section") )
							showLine = true;

					}

					if ( showLine )
						filtered += l + "\n";
				}

				return filtered;
			}
		};

		System.out.println(string);
		System.out.println();

		OptionParser p = buildOptionParser();
		p.formatHelpWith(filteredHelpFormatter);
		try {
			p.printHelpOn(System.out);
			System.exit(1);
		} catch (IOException e) { }
	}

	protected Properties readPropertiesFile(String filename, Boolean abortOnMissing) {
		Properties p = null;
		File file = new File(filename);
		if ( !file.exists() ) {
			if ( abortOnMissing ) {
				System.err.println("Couldn't find config file: " + filename);
				System.exit(1);
			} else {
				return null;
			}
		}

		try {
			FileReader reader = new FileReader(file);
			p = new Properties();
			p.load(reader);
			for (Object key : p.keySet()) {
				LOGGER.debug("Got config key: {}", key);
			}
		} catch ( IOException e ) {
			System.err.println("Couldn't read config file: " + e);
			System.exit(1);
		}
		return p;
	}

	protected Properties readPropertiesEnv(String envConfig) {
		LOGGER.debug("Attempting to read env_config param: {}", envConfig);
		String envConfigJsonRaw = System.getenv(envConfig);
		if (envConfigJsonRaw != null && envConfigJsonRaw.trim().startsWith("{")) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				Map<String, Object> stringMap = mapper.readValue(envConfigJsonRaw, Map.class);
				Properties properties = new Properties();
				for (Map.Entry<String, Object> entry : stringMap.entrySet()) {
					LOGGER.debug("Got env_config key: {}", entry.getKey());
					if (entry.getKey() != null && entry.getValue() != null) {
						properties.put(entry.getKey(), entry.getValue().toString());
					}
				}
				return properties;
			} catch (JsonProcessingException e) {
				throw new IllegalArgumentException("Unparseable JSON in env variable " + envConfig, e);
			}
		} else {
			System.err.println("No JSON-encoded environment variable named: " + envConfig);
			System.exit(1);
			throw new IllegalArgumentException("No JSON-encoded environment variable named: " + envConfig);
		}
	}

	protected Object fetchOption(String name, OptionSet options, Properties properties, Object defaultVal) {
		if ( options != null && options.has(name) )
			return options.valueOf(name);
		else if ( (properties != null) && properties.containsKey(name) )
			return properties.getProperty(name);
		else
			return defaultVal;
	}

	protected String fetchStringOption(String name, OptionSet options, Properties properties, String defaultVal) {
		return (String) fetchOption(name, options, properties, defaultVal);
	}

	private boolean parseBoolean(String name, String value) {
		if ( !"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
			usageForOptions("Invalid value for " + name + ", true|false required", "--" + name);
			return false;
		}
		return Boolean.parseBoolean(value);
	}

	protected boolean fetchBooleanOption(String name, OptionSet options, Properties properties, boolean defaultVal) {
		if (options != null && options.has(name)) {
			if (!options.hasArgument(name))
				return true;
			else {
				Object o = options.valueOf(name);
				if (o instanceof String) {
					return parseBoolean(name, (String) o);
				} else {
					return (boolean) o;
				}
			}
		} else if ((properties != null) && properties.containsKey(name))
			return parseBoolean(name, properties.getProperty(name));
		else
			return defaultVal;
	}

	protected Long fetchLongOption(String name, OptionSet options, Properties properties, Long defaultVal) {
		try {
			Object opt = fetchOption(name, options, properties, defaultVal);
			if ( opt instanceof String)
				return Long.valueOf((String) opt);
			else
				return (Long) opt;
		} catch ( NumberFormatException|OptionException e ) {
			usageForOptions("Invalid value for " + name + ", number required", "--" + name);
		}

		return null; // never reached
	}

	protected Integer fetchIntegerOption(String name, OptionSet options, Properties properties, Integer defaultVal) {
		try {
			Object opt = fetchOption(name, options, properties, defaultVal);
			if ( opt instanceof String)
				return Integer.valueOf((String) opt);
			else
				return (Integer) opt;
		} catch ( NumberFormatException|OptionException e ) {
			usageForOptions("Invalid value for " + name + ", number required", "--" + name);
		}

		return null; // never reached
	}

	protected Float fetchFloatOption(String name, OptionSet options, Properties properties, Float defaultVal) {
		try {
			Object opt = fetchOption(name, options, properties, defaultVal);
			if ( opt instanceof String)
				return Float.valueOf((String) opt);
			else
				return (Float) opt;
		} catch ( NumberFormatException|OptionException e ) {
			usageForOptions("Invalid value for " + name + ", float required", "--" + name);
		}

		return null; // never reached
	}


	protected MaxwellMysqlConfig parseMysqlConfig(String prefix, OptionSet options, Properties properties) {
		MaxwellMysqlConfig config = new MaxwellMysqlConfig();
		config.host     = fetchStringOption(prefix + "host", options, properties, null);
		config.password = fetchStringOption(prefix + "password", options, properties, null);
		config.user     = fetchStringOption(prefix + "user", options, properties, null);
		config.port     = fetchIntegerOption(prefix + "port", options, properties, 3306);
		config.sslMode  = this.getSslModeFromString(fetchStringOption(prefix + "ssl", options, properties, null));
		config.setJDBCOptions(
		    fetchStringOption(prefix + "jdbc_options", options, properties, null));

		// binlog_heartbeat isn't prefixed, as it only affects replication
		config.enableHeartbeat = fetchBooleanOption("binlog_heartbeat", options, properties, config.enableHeartbeat);
		return config;
	}

	private SSLMode getSslModeFromString(String sslMode) {
		if (sslMode != null) {
			for (SSLMode mode : SSLMode.values()) {
				if (mode.toString().equals(sslMode)) {
					return mode;
				}
			}
			System.err.println("Invalid binlog SSL mode string: " + sslMode);
			System.exit(1);
		}
		return null;
	}
}
