package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.api.config.ConfigurationSupport;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.Properties;

public abstract class AbstractConfigurationOptionMerger implements ConfigurationOptionMerger {
	public static final String ENV_CONFIG_PREFIX = "env_config_prefix";

	private final ConfigurationFileParser configurationFileParser;
	private final ConfigurationSupport configurationSupport;

	protected AbstractConfigurationOptionMerger(ConfigurationFileParser configurationFileParser, ConfigurationSupport configurationSupport) {
		this.configurationFileParser = configurationFileParser;
		this.configurationSupport = configurationSupport;
	}

	protected Properties readConfigurationFile(OptionSet options) {
		if (options.has("config")) {
			return configurationFileParser.parseFile((String) options.valueOf("config"), true);
		} else {
			return configurationFileParser.parseFile(ConfigurationSupport.DEFAULT_CONFIG_FILE, false);
		}
	}

	protected void overwriteConfigurationFileSettingWithEnvironmentVariables(final OptionSet optionSet, final Properties properties) {
		String envConfigPrefix = optionSet.has(ENV_CONFIG_PREFIX) ? (String)optionSet.valueOf(ENV_CONFIG_PREFIX) : configurationSupport.fetchOption(ENV_CONFIG_PREFIX, properties, null);
		if (envConfigPrefix != null) {
			String prefix = envConfigPrefix.toLowerCase();
			System.getenv().entrySet().stream()
					.filter(map -> map.getKey().toLowerCase().startsWith(prefix))
					.forEach(config -> properties.put(config.getKey().toLowerCase().replaceFirst(prefix, ""), config.getValue()));
		}
	}

	protected Properties mergeCommandLineParametersAndConfiguration(final OptionSet options, final Properties properties) {
		final Properties result = new Properties();

		if(options != null){
			for(OptionSpec<?> spec : options.specs()){
				String value = (String)options.valueOf(spec);
				for(String option : spec.options()){
					result.put(option, value);
				}
			}
		}

		if(properties != null){
			properties.forEach((k,v) -> {
				if(!result.containsKey(k)) {
					result.put(k, v);
				}
			});
		}

		return result;
	}
}
