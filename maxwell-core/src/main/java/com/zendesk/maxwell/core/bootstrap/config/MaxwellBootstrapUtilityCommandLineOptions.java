package com.zendesk.maxwell.core.bootstrap.config;

import com.zendesk.maxwell.core.config.AbstractCommandLineOptions;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;

import java.util.Map;

public class MaxwellBootstrapUtilityCommandLineOptions extends AbstractCommandLineOptions  {

	@Override
	public OptionParser createParser() {OptionParser parser = new OptionParser();
		parser.accepts( "config", "location of config file" ).withRequiredArg();
		parser.accepts( "__separator_1", "" );
		parser.accepts( "database", "database that contains the table to bootstrap").withRequiredArg();
		parser.accepts( "table", "table to bootstrap").withRequiredArg();
		parser.accepts( "where", "where clause to restrict the rows bootstrapped from the specified table. e.g. my_date >= '2017-01-01 11:07:13'").withOptionalArg();
		parser.accepts( "__separator_2", "" );
		parser.accepts( "abort", "bootstrap_id to abort" ).withRequiredArg();
		parser.accepts( "monitor", "bootstrap_id to monitor" ).withRequiredArg();
		parser.accepts( "__separator_3", "" );
		parser.accepts( "client_id", "maxwell client to perform the bootstrap" ).withRequiredArg();
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR. default: WARN" ).withRequiredArg();
		parser.accepts( "host", "mysql host. default: localhost").withRequiredArg();
		parser.accepts( "user", "mysql username. default: maxwell" ).withRequiredArg();
		parser.accepts( "password", "mysql password" ).withRequiredArg();
		parser.accepts( "port", "mysql port. default: 3306" ).withRequiredArg();
		parser.accepts( "schema_database", "database that contains maxwell schema and state").withRequiredArg();
		parser.accepts( "help", "display help").forHelp();

		BuiltinHelpFormatter helpFormatter = new BuiltinHelpFormatter(200, 4) {
			@Override
			public String format(Map<String, ? extends OptionDescriptor> options) {
				this.addRows(options.values());
				String output = this.formattedHelpOutput();
				return output.replaceAll("--__separator_.*", "");
			}
		};
		parser.formatHelpWith(helpFormatter);
		return parser;
	}

}
