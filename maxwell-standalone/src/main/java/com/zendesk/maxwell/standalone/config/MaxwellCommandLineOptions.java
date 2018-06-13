package com.zendesk.maxwell.standalone.config;

import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.metricreporter.core.MetricReporterConfigurator;
import com.zendesk.maxwell.core.producer.ProducerConfigurator;
import com.zendesk.maxwell.standalone.SpringLauncher;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.regex.Pattern;

@Service
public class MaxwellCommandLineOptions extends AbstractCommandLineOptions {

	private static MaxwellCommandLineOptions INSTANCE;

	public static MaxwellCommandLineOptions getInstance(){
		if(INSTANCE == null){
			SpringLauncher.launch(ctx -> INSTANCE = ctx.getBean(MaxwellCommandLineOptions.class));
		}
		return INSTANCE;
	}

	private final List<ProducerConfigurator> producerConfigurators;
	private final List<MetricReporterConfigurator> metricReporterConfigurators;

	@Autowired
	public MaxwellCommandLineOptions(List<ProducerConfigurator> producerConfigurators, Optional<List<MetricReporterConfigurator>> metricReporterConfigurators) {
		this.producerConfigurators = producerConfigurators;
		this.metricReporterConfigurators = metricReporterConfigurators.orElseGet(ArrayList::new);
	}

	@PostConstruct
	public void setInstance(){
		INSTANCE = this;
	}

	@Override
	protected OptionParser createParser() {
		final OptionParser parser = new OptionParser();
		addCommandLineOptions(parser);

		parser.accepts( "help", "display help" ).forHelp();
		BuiltinHelpFormatter helpFormatter = new BuiltinHelpFormatter(200, 4) {
			@Override
			public String format(Map<String, ? extends OptionDescriptor> options) {
				this.addRows(options.values());
				String output = this.formattedHelpOutput();
				output = output.replaceAll("--__separator_.*", "");

				Pattern deprecated = Pattern.compile("^.*\\[deprecated\\].*\\n", Pattern.MULTILINE);
				return deprecated.matcher(output).replaceAll("");
			}
		};

		parser.formatHelpWith(helpFormatter);
		return parser;
	}

	private void addCommandLineOptions(OptionParser parser) {
		final CommandLineOptionParserContext context = new BaseCommandLineOptionParserContext(parser);

		context.addOptionWithRequiredArgument( "config", "location of config file" );
		context.addOptionWithRequiredArgument( "env_config_prefix", "prefix of env var based config, case insensitive" );
		context.addOptionWithRequiredArgument( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR" );
		context.addOptionWithOptionalArgument( "daemon", "daemon, running maxwell as a daemon" );

		context.addSeparator();

		context.addOptionWithRequiredArgument( "host", "mysql host with write access to maxwell database" );
		context.addOptionWithRequiredArgument( "port", "port for host" );
		context.addOptionWithRequiredArgument( "user", "username for host" );
		context.addOptionWithRequiredArgument( "password", "password for host" );
		context.addOptionWithRequiredArgument( "jdbc_options", "additional jdbc connection options" );
		context.addOptionWithRequiredArgument( "binlog_connector", "[deprecated]" );

		context.addOptionWithOptionalArgument( "ssl", "enables SSL for all connections: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY. default: DISABLED");
		context.addOptionWithOptionalArgument( "replication_ssl", "overrides SSL setting for binlog connection: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY");
		context.addOptionWithOptionalArgument( "schema_ssl", "overrides SSL setting for schema capture connection: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY");

		context.addSeparator();

		context.addOptionWithRequiredArgument( "replication_host", "mysql host to replicate from (if using separate schema and replication servers)" );
		context.addOptionWithRequiredArgument( "replication_user", "username for replication_host" );
		context.addOptionWithRequiredArgument( "replication_password", "password for replication_host" );
		context.addOptionWithRequiredArgument( "replication_port", "port for replication_host" );

		context.addOptionWithRequiredArgument( "schema_host", "overrides replication_host for retrieving schema" );
		context.addOptionWithRequiredArgument( "schema_user", "username for schema_host" );
		context.addOptionWithRequiredArgument( "schema_password", "password for schema_host" );
		context.addOptionWithRequiredArgument( "schema_port", "port for schema_host" );

		context.addSeparator();

		context.addOptionWithRequiredArgument( "producer", "producer type: stdout|file|kafka|kinesis|pubsub|sqs|rabbitmq|redis" );
		context.addOptionWithRequiredArgument( "custom_producer.factory", "fully qualified custom producer factory class" );
		context.addOptionWithRequiredArgument( "producer_ack_timeout", "producer message acknowledgement timeout" );
		context.addOptionWithRequiredArgument( "output_file", "output file for 'file' producer" );

		context.addOptionWithRequiredArgument( "producer_partition_by", "database|table|primary_key|column, kafka/kinesis producers will partition by this value");
		context.addOptionWithRequiredArgument("producer_partition_columns",
				"with producer_partition_by=column, partition by the value of these columns.  "
						+ "comma separated.");
		context.addOptionWithRequiredArgument( "producer_partition_by_fallback", "database|table|primary_key, fallback to this value when using 'column' partitioning and the columns are not present in the row");

		context.addSeparator();

		context.addOptionWithRequiredArgument( "output_binlog_position", "produced records include binlog position; [true|false]. default: false" );
		context.addOptionWithRequiredArgument( "output_gtid_position", "produced records include gtid position; [true|false]. default: false" );
		context.addOptionWithRequiredArgument( "output_commit_info", "produced records include commit and xid; [true|false]. default: true" );
		context.addOptionWithRequiredArgument( "output_xoffset", "produced records include xoffset, option \"output_commit_info\" must be enabled; [true|false]. default: false" );
		context.addOptionWithRequiredArgument( "output_nulls", "produced records include fields with NULL values [true|false]. default: true" );
		context.addOptionWithRequiredArgument( "output_server_id", "produced records include server_id; [true|false]. default: false" );
		context.addOptionWithRequiredArgument( "output_thread_id", "produced records include thread_id; [true|false]. default: false" );
		context.addOptionWithRequiredArgument( "output_row_query", "produced records include query, binlog option \"binlog_rows_query_log_events\" must be enabled; [true|false]. default: false" );
		context.addOptionWithRequiredArgument( "output_ddl", "produce DDL records to ddl_kafka_topic [true|false]. default: false" );
		context.addOptionWithRequiredArgument( "ddl_kafka_topic", "optionally provide an alternate topic to push DDL records to. default: kafka_topic" );
		context.addOptionWithRequiredArgument("secret_key", "The secret key for the AES encryption" );
		context.addOptionWithRequiredArgument("encrypt", "encryption mode: [none|data|all]. default: none" );

		context.addSeparator();

		context.addOptionWithRequiredArgument( "bootstrapper", "bootstrapper type: async|sync|none. default: async" );

		context.addSeparator();

		context.addOptionWithRequiredArgument( "replica_server_id", "server_id that maxwell reports to the master.  See docs for full explanation. ");
		context.addOptionWithRequiredArgument( "client_id", "unique identifier for this maxwell replicator" );
		context.addOptionWithRequiredArgument( "schema_database", "database name for maxwell state (schema and binlog position)" );
		context.addOptionWithRequiredArgument( "max_schemas", "[deprecated]" );
		context.addOptionWithRequiredArgument( "init_position", "initial binlog position, given as BINLOG_FILE:POSITION[:HEARTBEAT]" );
		context.addOptionWithRequiredArgument( "replay", "replay mode, don't store any information to the server" );
		context.addOptionWithRequiredArgument( "master_recovery", "(experimental) enable master position recovery code" );
		context.addOptionWithRequiredArgument( "gtid_mode", "(experimental) enable gtid mode" );
		context.addOptionWithRequiredArgument( "ignore_producer_error", "Maxwell will be terminated on kafka/kinesis errors when false. Otherwise, those producer errors are only logged. Default to true" );

		context.addSeparator();

		context.addOptionWithRequiredArgument( "include_dbs", "include these databases, formatted as include_dbs=db1,db2" );
		context.addOptionWithRequiredArgument( "exclude_dbs", "exclude these databases, formatted as exclude_dbs=db1,db2" );
		context.addOptionWithRequiredArgument( "include_tables", "include these tables, formatted as include_tables=db1,db2" );
		context.addOptionWithRequiredArgument( "exclude_tables", "exclude these tables, formatted as exclude_tables=tb1,tb2" );
		context.addOptionWithRequiredArgument( "exclude_columns", "exclude these columns, formatted as exclude_columns=col1,col2" );
		context.addOptionWithRequiredArgument( "blacklist_dbs", "ignore data AND schema changes to these databases, formatted as blacklist_dbs=db1,db2. See the docs for details before setting this!" );
		context.addOptionWithRequiredArgument( "blacklist_tables", "ignore data AND schema changes to these tables, formatted as blacklist_tables=tb1,tb2. See the docs for details before setting this!" );
		context.addOptionWithRequiredArgument( "include_column_values", "include only rows with these values formatted as include_column_values=C=x,D=y" );

		context.addSeparator();

		producerConfigurators.stream().sorted(Comparator.comparing(ProducerConfigurator::getIdentifier)).forEach(c -> {
			c.configureCommandLineOptions(context);
			context.addSeparator();
		});

		context.addOptionWithRequiredArgument( "metrics_prefix", "the prefix maxwell will apply to all metrics" );
		context.addOptionWithRequiredArgument( "metrics_type", "how maxwell metrics will be reported, at least one of slf4j|jmx|http|datadog" );
		context.addOptionWithRequiredArgument( "metrics_jvm", "enable jvm metrics: true|false. default: false" );

		metricReporterConfigurators.stream().sorted(Comparator.comparing(MetricReporterConfigurator::getIdentifier)).forEach(c -> {
			c.configureCommandLineOptions(context);
			context.addSeparator();
		});

		context.addSeparator();
	}

}
