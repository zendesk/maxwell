package com.zendesk.maxwell.replay;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.util.MaxwellOptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author udyr@shlaji.com
 */
public class ReplayConfig extends MaxwellConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplayFile.class);

	List<File> binlogFiles = new ArrayList<>();

	public ReplayConfig(String[] args) {
		super(args);
		this.parse(args);

		// No heartbeat, No schema
		replayMode = true;
	}

	private void parse(String[] args) {
		MaxwellOptionParser parser = buildOptionParser();
		OptionSet options = parser.parse(args);

		final Properties properties;

		if (options.has("config")) {
			properties = parseFile((String) options.valueOf("config"), true);
		} else {
			properties = parseFile(DEFAULT_CONFIG_FILE, false);
		}

		if (options.has("env_config")) {
			Properties envConfigProperties = readPropertiesEnv((String) options.valueOf("env_config"));
			for (Map.Entry<Object, Object> entry : envConfigProperties.entrySet()) {
				Object key = entry.getKey();
				if (properties.put(key, entry.getValue()) != null) {
					LOGGER.debug("Replaced config key {} with value from env_config", key);
				}
			}
		}

		String envConfigPrefix = fetchStringOption("env_config_prefix", options, properties, null);

		if (envConfigPrefix != null) {
			String prefix = envConfigPrefix.toLowerCase();
			System.getenv().entrySet().stream()
					.filter(map -> map.getKey().toLowerCase().startsWith(prefix))
					.forEach(config -> {
						String rawKey = config.getKey();
						String newKey = rawKey.toLowerCase().replaceFirst(prefix, "");
						if (properties.put(newKey, config.getValue()) != null) {
							LOGGER.debug("Got env variable {} and replacing config key {}", rawKey, newKey);
						} else {
							LOGGER.debug("Got env variable {} as config key {}", rawKey, newKey);
						}
					});
		}

		String replayBinlog = fetchStringOption("replay_binlog", options, properties, null);
		if (StringUtils.isBlank(replayBinlog)) {
			usage("Please specify a replay_binlog: ", parser, "replay");
		}
		this.binlogFiles.addAll(validateReplayFiles(replayBinlog, parser));
	}

	private List<File> validateReplayFiles(String replayBinlog, MaxwellOptionParser parser) {
		return Arrays.stream(replayBinlog.split(",")).map(File::new).filter(file -> {
			if (file.exists() && file.isFile()) {
				return true;
			} else {
				usage("No files were found available through your configuration, check: " + file.getPath(), parser, "replay");
				return false;
			}
		}).collect(Collectors.toList());
	}

	private Properties parseFile(String filename, boolean abortOnMissing) {
		return this.readPropertiesFile(filename, abortOnMissing);
	}

	@Override
	protected MaxwellOptionParser buildOptionParser() {
		MaxwellOptionParser parser = super.buildOptionParser();
		parser.section("replay");
		parser.accepts("replay_binlog", "binlog file").withRequiredArg();
		return parser;
	}
}
