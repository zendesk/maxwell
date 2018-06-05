package com.zendesk.maxwell.core.producer.impl.file;

import com.zendesk.maxwell.core.producer.ProducerConfiguration;
import com.zendesk.maxwell.core.config.InvalidOptionException;

public class FileProducerConfiguration implements ProducerConfiguration {
	private final String outputFile;

	public FileProducerConfiguration(String outputFile) {
		this.outputFile = outputFile;
	}

	public String getOutputFile() {
		return outputFile;
	}

	@Override
	public void validate() {
		if (outputFile == null) {
			throw new InvalidOptionException("please specify --output_file=FILE to use the file producer", "--producer", "--output_file");
		}
	}
}
