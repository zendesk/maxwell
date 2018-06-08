package com.zendesk.maxwell.standalone.config;

import com.zendesk.maxwell.api.config.InvalidUsageException;
import joptsimple.OptionSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class MaxwellConfigurationOptionMerger extends AbstractConfigurationOptionMerger {

	private final MaxwellCommandLineOptions maxwellCommandLineOptions;

	@Autowired
	public MaxwellConfigurationOptionMerger(MaxwellCommandLineOptions maxwellCommandLineOptions, ConfigurationFileParser configurationFileParser) {
		super(configurationFileParser);
		this.maxwellCommandLineOptions = maxwellCommandLineOptions;
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
	@Override
	public Properties mergeCommandLineOptionsWithConfigurationAndSystemEnvironment(String[] args){
		OptionSet options = maxwellCommandLineOptions.parse(args);
		if (options.has("help")){
			throw new InvalidUsageException("Help for Maxwell:");
		}

		Properties properties = readConfigurationFile(options);
		overwriteConfigurationFileSettingWithEnvironmentVariables(options, properties);

		return mergeCommandLineParametersAndConfiguration(options, properties);
	}



}
