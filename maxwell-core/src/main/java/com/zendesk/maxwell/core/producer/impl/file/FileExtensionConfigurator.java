package com.zendesk.maxwell.core.producer.impl.file;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.ExtensionConfigurator;
import com.zendesk.maxwell.core.config.ExtensionType;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerInstantiationException;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class FileExtensionConfigurator implements ExtensionConfigurator<Producer> {
	@Override
	public String getExtensionIdentifier() {
		return "file";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PROVIDER;
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		try {
			return new FileProducer(context, context.getConfig().getOutputFile());
		} catch (IOException e) {
			throw new ProducerInstantiationException(e);
		}
	}
}
