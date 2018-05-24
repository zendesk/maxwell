package com.zendesk.maxwell.core.config;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ConfigurationFileParser {
	
	public Properties parseFile(String filename, boolean abortOnMissing) {
		Properties p = readPropertiesFile(filename, abortOnMissing);

		if ( p == null )
			p = new Properties();

		return p;
	}

	private Properties readPropertiesFile(String filename, Boolean abortOnMissing) {
		Properties p = null;
		File file = new File(filename);
		if ( !file.exists() ) {
			if ( abortOnMissing ) {
				System.err.println("Couldn't find config file: " + filename);
				System.exit(1);
			} else {
				return null;
			}
		}

		try {
			FileReader reader = new FileReader(file);
			p = new Properties();
			p.load(reader);
		} catch ( IOException e ) {
			System.err.println("Couldn't read config file: " + e);
			System.exit(1);
		}
		return p;
	}
	
}
