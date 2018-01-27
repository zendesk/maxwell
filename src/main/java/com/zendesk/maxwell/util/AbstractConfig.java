package com.zendesk.maxwell.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.*;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import joptsimple.*;

import com.zendesk.maxwell.MaxwellMysqlConfig;

public abstract class AbstractConfig {
	static final protected String DEFAULT_CONFIG_FILE = "config.properties";
	protected abstract OptionParser buildOptionParser();

	protected void usage(String string) {
		System.err.println(string);
		System.err.println();
		try {
			buildOptionParser().printHelpOn(System.err);
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
						if ( l.contains(o) )
							showLine = true;
					}

					if ( showLine )
						filtered += l + "\n";
				}

				return filtered;
			}
		};

		System.err.println(string);
		System.err.println();

		OptionParser p = buildOptionParser();
		p.formatHelpWith(filteredHelpFormatter);
		try {
			p.printHelpOn(System.err);
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
		} catch ( IOException e ) {
			System.err.println("Couldn't read config file: " + e);
			System.exit(1);
		}
		return p;
	}

	protected String fetchOption(String name, OptionSet options, Properties properties, String defaultVal) {
		if ( options != null && options.has(name) )
			return (String) options.valueOf(name);
		else if ( (properties != null) && properties.containsKey(name) )
			return properties.getProperty(name);
		else
			return defaultVal;
	}


	protected boolean fetchBooleanOption(String name, OptionSet options, Properties properties, boolean defaultVal) {
		if ( options != null && options.has(name) ) {
			if ( !options.hasArgument(name) )
				return true;
			else
				return Boolean.valueOf((String) options.valueOf(name));
		} else if ( (properties != null) && properties.containsKey(name) )
			return Boolean.valueOf(properties.getProperty(name));
		else
			return defaultVal;
	}

	protected Long fetchLongOption(String name, OptionSet options, Properties properties, Long defaultVal) {
		String strOption = fetchOption(name, options, properties, null);
		if ( strOption == null )
			return defaultVal;
		else {
			try {
				return Long.valueOf(strOption);
			} catch ( NumberFormatException e ) {
				usageForOptions("Invalid value for " + name + ", integer required", "--" + name);
			}
			return null; // unreached
		}
	}


	protected MaxwellMysqlConfig parseMysqlConfig(String prefix, OptionSet options, Properties properties) {
		MaxwellMysqlConfig config = new MaxwellMysqlConfig();
		config.host     = fetchOption(prefix + "host", options, properties, null);
		config.password = fetchOption(prefix + "password", options, properties, null);
		config.user     = fetchOption(prefix + "user", options, properties, null);
		config.port     = Integer.valueOf(fetchOption(prefix + "port", options, properties, "3306"));
		config.sslMode  = this.getSslModeFromString(fetchOption(prefix + "ssl", options, properties, null));
		config.setJDBCOptions(
		    fetchOption(prefix + "jdbc_options", options, properties, null));
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
