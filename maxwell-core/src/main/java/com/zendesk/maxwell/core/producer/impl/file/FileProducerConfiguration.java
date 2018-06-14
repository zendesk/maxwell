package com.zendesk.maxwell.core.producer.impl.file;

import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;

public class FileProducerConfiguration implements ProducerConfiguration {
	public final String outputFile;

	public FileProducerConfiguration(String outputFile) {
		this.outputFile = outputFile;
	}

	@Override
	public void validate() {
		if (outputFile == null) {
			throw new InvalidOptionException("please specify --output_file=FILE to use the file producer", "--producer", "--output_file");
		}
	}
}
