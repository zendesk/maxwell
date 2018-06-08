package com.zendesk.maxwell.standalone.config;

import com.zendesk.maxwell.core.config.CommandLineOptions;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractCommandLineOptions implements CommandLineOptions {

	private static OptionParser parser;

	@Override
	public OptionSet parse(String[] args){
		return getParser().parse(args);
	}

	@Override
	public OptionParser getParser(){
		if(parser == null){
			parser = createParser();
		}
		return parser;
	}

	@Override
	public void usage(String string) {
		System.err.println(string);
		System.err.println();
		try {
			getParser().printHelpOn(System.err);
		} catch (IOException e) { }
	}

	@Override
	public void usageForOptions(String string, final String... filterOptions) {
		BuiltinHelpFormatter filteredHelpFormatter = new BuiltinHelpFormatter(200, 4) {
			@Override
			public String format(Map<String, ? extends OptionDescriptor> options) {
				this.addRows(options.values());
				String output = this.formattedHelpOutput();
				String[] lines = output.split("\n");

				String filtered = "";
				int i = 0;
				for ( String l : lines ) {
					boolean showLine = false;

					if ( l.contains("--help") || i++ < 2 ) // take the first 3 lines, these are the header
						showLine = true;
					for ( String o : filterOptions )  {
						if ( l.contains(o) )
							showLine = true;
					}

					if ( showLine )
						filtered += l + "\n";
				}

				return filtered;
			}
		};

		System.err.println(string);
		System.err.println();

		OptionParser p = getParser();
		p.formatHelpWith(filteredHelpFormatter);
		try {
			p.printHelpOn(System.err);
		} catch (IOException e) { }
	}

	protected abstract OptionParser createParser();

}
