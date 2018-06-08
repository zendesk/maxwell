package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import com.zendesk.maxwell.api.producer.PropertiesProducerConfiguration;
import com.zendesk.maxwell.core.config.BaseMaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.config.MaxwellConfigurationOptionMerger;
import com.zendesk.maxwell.core.producer.impl.noop.NoopProducerFactory;
import com.zendesk.maxwell.core.util.Logging;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

@Service
public class MaxwellConfigurator {
	private static final String NONE_PRODUCER_TYPE = "none";

	private final ConfigurationSupport configurationSupport;
	private final MaxwellConfigurationOptionMerger maxwellConfigurationOptionMerger;
	private final MaxwellConfigFactory maxwellConfigFactory;
	private final List<ProducerConfigurator> producerConfigurators;
	private final Logging logging;

	@Autowired
	public MaxwellConfigurator(ConfigurationSupport configurationSupport,
							   MaxwellConfigurationOptionMerger maxwellConfigurationOptionMerger,
							   MaxwellConfigFactory maxwellConfigFactory,
							   List<ProducerConfigurator> producerConfigurators,
							   Logging logging) {
		this.configurationSupport = configurationSupport;
		this.maxwellConfigurationOptionMerger = maxwellConfigurationOptionMerger;
		this.maxwellConfigFactory = maxwellConfigFactory;
		this.producerConfigurators = producerConfigurators;
		this.logging = logging;
	}

	public MaxwellConfig prepareMaxwell(String[] commandlineArguments){
		final Properties configurationOptions = maxwellConfigurationOptionMerger.merge(commandlineArguments);
		setupLogging(configurationOptions);
		BaseMaxwellConfig config = maxwellConfigFactory.createFor(configurationOptions);
		appendProducerConfiguration(config, configurationOptions);
		return config;
	}

	private void setupLogging(final Properties configurationOptions){
		String logLevel = configurationSupport.fetchOption("log_level", configurationOptions, null);
		if(logLevel != null){
			logging.setLevel(logLevel);
		}
	}

	private void appendProducerConfiguration(BaseMaxwellConfig maxwellConfig, Properties configurationOptions) {
		final String producerType = maxwellConfig.getProducerType();
		if(producerType != null){
			if(NONE_PRODUCER_TYPE.equalsIgnoreCase(producerType)){
				maxwellConfig.setProducerConfiguration(new PropertiesProducerConfiguration());
				maxwellConfig.setProducerFactory(NoopProducerFactory.class.getCanonicalName());
			}else {
				ProducerConfigurator configurator = getProducerConfigurator(producerType);
				maxwellConfig.setProducerConfiguration(configurator.parseConfiguration(configurationOptions).orElseGet(PropertiesProducerConfiguration::new));
				maxwellConfig.setProducerFactory(configurator.getFactory().getCanonicalName());
			}
		}
	}

	private ProducerConfigurator getProducerConfigurator(String type){
		return producerConfigurators.stream()
				.filter(c -> type.equalsIgnoreCase(c.getIdentifier()))
				.findFirst()
				.orElseThrow(() -> new InvalidOptionException("No producer available of type " +  type, "--producer_type"));
	}

}
