package com.zendesk.maxwell.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.*;
import joptsimple.*;

public abstract class AbstractConfig {
	static final protected String DEFAULT_CONFIG_FILE = "config.properties";
	protected abstract OptionParser buildOptionParser();

	protected void usage(String string) {
		System.err.println(string);
		System.err.println();
		try {
			buildOptionParser().printHelpOn(System.err);
			System.exit(1);
		} catch (IOException e) { }
	}

	protected Properties readPropertiesFile(String filename, Boolean abortOnMissing) {
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
