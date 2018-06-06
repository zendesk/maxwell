package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.api.config.InvalidUsageException;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

@Service
public class MaxwellConfigurationOptionMerger {
	public static final String ENV_CONFIG_PREFIX = "env_config_prefix";

	private final MaxwellCommandLineOptions maxwellCommandLineOptions;
	private final ConfigurationFileParser configurationFileParser;
	private final ConfigurationSupport configurationSupport;

	@Autowired
	public MaxwellConfigurationOptionMerger(MaxwellCommandLineOptions maxwellCommandLineOptions, ConfigurationFileParser configurationFileParser, ConfigurationSupport configurationSupport) {
		this.maxwellCommandLineOptions = maxwellCommandLineOptions;
		this.configurationFileParser = configurationFileParser;
		this.configurationSupport = configurationSupport;
	}

	/**
	 * Merges the configuration options from the configuration file, with the settings from environment variables and
	 * command line parameters. The command line parameters have the highest priority followed by the environment
	 * variables and then the settings from the configuration file.
	 *
	 * By default environment variables are prefixed with 'MAXWELL_' this option can be overwritten with the command
	 * line or configuration file option 'env_config_prefix'.
	 * The default configuration file is called 'config.properties'. This can be customized by the command line option
	 * 'config'.
	 *
	 * @param args
	 * @return
	 */
	public Properties merge(String[] args){
		OptionSet options = maxwellCommandLineOptions.parse(args);
		if (options.has("help")){
			throw new InvalidUsageException("Help for Maxwell:");
		}

		List<?> arguments = options.nonOptionArguments();
		if (!arguments.isEmpty()) {
			throw new InvalidUsageException("Unknown argument(s): " + arguments);
		}

		Properties properties = readConfigurationFile(options);
		overwriteConfigurationFileSettingWithEnvironmentVariables(options, properties);

		return mergeCommandLineParametersAndConfiguration(options, properties);
	}

	private Properties readConfigurationFile(OptionSet options) {
		if (options.has("config")) {
			return configurationFileParser.parseFile((String) options.valueOf("config"), true);
		} else {
			return configurationFileParser.parseFile(ConfigurationSupport.DEFAULT_CONFIG_FILE, false);
		}
	}

	private void overwriteConfigurationFileSettingWithEnvironmentVariables(final OptionSet optionSet, final Properties properties) {
		String envConfigPrefix = optionSet.has(ENV_CONFIG_PREFIX) ? (String)optionSet.valueOf(ENV_CONFIG_PREFIX) : configurationSupport.fetchOption(ENV_CONFIG_PREFIX, properties, null);
		if (envConfigPrefix != null) {
			String prefix = envConfigPrefix.toLowerCase();
			System.getenv().entrySet().stream()
					.filter(map -> map.getKey().toLowerCase().startsWith(prefix))
					.forEach(config -> properties.put(config.getKey().toLowerCase().replaceFirst(prefix, ""), config.getValue()));
		}
	}

	private Properties mergeCommandLineParametersAndConfiguration(final OptionSet options, final Properties properties) {
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
