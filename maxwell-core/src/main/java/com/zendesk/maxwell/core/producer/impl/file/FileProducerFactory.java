package com.zendesk.maxwell.core.producer.impl.file;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.NamedProducerFactory;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerInstantiationException;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class FileProducerFactory implements NamedProducerFactory {
	@Override
	public String getName() {
		return "file";
	}

	@Override
	public Producer createProducer(MaxwellContext context) {
		try {
			return new FileProducer(context, context.getConfig().outputFile);
		} catch (IOException e) {
			throw new ProducerInstantiationException(e);
		}
	}
}
