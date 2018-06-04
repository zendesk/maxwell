package com.zendesk.maxwell.core.producer.impl.file;

import com.zendesk.maxwell.core.config.ExtensionConfiguration;
import com.zendesk.maxwell.core.config.InvalidOptionException;
import com.zendesk.maxwell.core.config.MaxwellConfig;

public class FileProducerConfiguration implements ExtensionConfiguration {
	private final String outputFile;

	public FileProducerConfiguration(String outputFile) {
		this.outputFile = outputFile;
	}

	public String getOutputFile() {
		return outputFile;
	}

	@Override
	public void validate(MaxwellConfig maxwellConfig) {
		if (outputFile == null) {
			throw new InvalidOptionException("please specify --output_file=FILE to use the file producer", "--producer", "--output_file");
		}
	}
}
