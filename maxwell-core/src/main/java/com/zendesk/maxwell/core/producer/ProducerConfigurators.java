package com.zendesk.maxwell.core.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProducerConfigurators {
	private final List<ProducerConfigurator> configurators;

	@Autowired
	public ProducerConfigurators(List<ProducerConfigurator> configurators) {
		this.configurators = configurators;
	}

	public ProducerConfigurator getByIdentifier(String identifier){
		return configurators.stream().filter(pc -> pc.getIdentifier().equals(identifier)).findFirst().orElseThrow(() -> new IllegalArgumentException("No producer of type " + identifier + " available"));
	}
}
