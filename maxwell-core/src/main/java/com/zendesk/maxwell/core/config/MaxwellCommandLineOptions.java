package com.zendesk.maxwell.core.config;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;

import java.util.Map;
import java.util.regex.Pattern;

public class MaxwellCommandLineOptions extends AbstractCommandLineOptions {

	@Override
	public OptionParser createParser() {
		final OptionParser parser = new OptionParser();
		parser.accepts( "config", "location of config file" ).withRequiredArg();
		parser.accepts( "env_config_prefix", "prefix of env var based config, case insensitive" ).withRequiredArg();
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR" ).withRequiredArg();
		parser.accepts( "daemon", "daemon, running maxwell as a daemon" ).withOptionalArg();

		parser.accepts("__separator_1");

		parser.accepts( "host", "mysql host with write access to maxwell database" ).withRequiredArg();
		parser.accepts( "port", "port for host" ).withRequiredArg();
		parser.accepts( "user", "username for host" ).withRequiredArg();
		parser.accepts( "password", "password for host" ).withRequiredArg();
		parser.accepts( "jdbc_options", "additional jdbc connection options" ).withRequiredArg();
		parser.accepts( "binlog_connector", "[deprecated]" ).withRequiredArg();

		parser.accepts( "ssl", "enables SSL for all connections: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY. default: DISABLED").withOptionalArg();
		parser.accepts( "replication_ssl", "overrides SSL setting for binlog connection: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY").withOptionalArg();
		parser.accepts( "schema_ssl", "overrides SSL setting for schema capture connection: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY").withOptionalArg();

		parser.accepts("__separator_2");

		parser.accepts( "replication_host", "mysql host to replicate from (if using separate schema and replication servers)" ).withRequiredArg();
		parser.accepts( "replication_user", "username for replication_host" ).withRequiredArg();
		parser.accepts( "replication_password", "password for replication_host" ).withRequiredArg();
		parser.accepts( "replication_port", "port for replication_host" ).withRequiredArg();

		parser.accepts( "schema_host", "overrides replication_host for retrieving schema" ).withRequiredArg();
		parser.accepts( "schema_user", "username for schema_host" ).withRequiredArg();
		parser.accepts( "schema_password", "password for schema_host" ).withRequiredArg();
		parser.accepts( "schema_port", "port for schema_host" ).withRequiredArg();

		parser.accepts("__separator_3");

		parser.accepts( "producer", "producer type: stdout|file|kafka|kinesis|pubsub|sqs|rabbitmq|redis" ).withRequiredArg();
		parser.accepts( "custom_producer.factory", "fully qualified custom producer factory class" ).withRequiredArg();
		parser.accepts( "producer_ack_timeout", "producer message acknowledgement timeout" ).withRequiredArg();
		parser.accepts( "output_file", "output file for 'file' producer" ).withRequiredArg();

		parser.accepts( "producer_partition_by", "database|table|primary_key|column, kafka/kinesis producers will partition by this value").withRequiredArg();
		parser.accepts("producer_partition_columns",
				"with producer_partition_by=column, partition by the value of these columns.  "
						+ "comma separated.").withRequiredArg();
		parser.accepts( "producer_partition_by_fallback", "database|table|primary_key, fallback to this value when using 'column' partitioning and the columns are not present in the row").withRequiredArg();

		parser.accepts( "kafka_version", "kafka client library version: 0.8.2.2|0.9.0.1|0.10.0.1|0.10.2.1|0.11.0.1").withRequiredArg();
		parser.accepts( "kafka_partition_by", "[deprecated]").withRequiredArg();
		parser.accepts( "kafka_partition_columns", "[deprecated]").withRequiredArg();
		parser.accepts( "kafka_partition_by_fallback", "[deprecated]").withRequiredArg();
		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" ).withRequiredArg();
		parser.accepts( "kafka_partition_hash", "default|murmur3, hash function for partitioning" ).withRequiredArg();
		parser.accepts( "kafka_topic", "optionally provide a topic name to push to. default: maxwell" ).withRequiredArg();
		parser.accepts( "kafka_key_format", "how to format the kafka key; array|hash" ).withRequiredArg();

		parser.accepts( "kinesis_stream", "kinesis stream name" ).withOptionalArg();
		parser.accepts( "sqs_queue_uri", "SQS Queue uri" ).withRequiredArg();

		parser.accepts( "pubsub_project_id", "provide a google cloud platform project id associated with the pubsub topic" ).withRequiredArg();
		parser.accepts( "pubsub_topic", "optionally provide a pubsub topic to push to. default: maxwell" ).withRequiredArg();
		parser.accepts( "ddl_pubsub_topic", "optionally provide an alternate pubsub topic to push DDL records to. default: pubsub_topic" ).withRequiredArg();

		parser.accepts("__separator_4");

		parser.accepts( "output_binlog_position", "produced records include binlog position; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_gtid_position", "produced records include gtid position; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_commit_info", "produced records include commit and xid; [true|false]. default: true" ).withOptionalArg();
		parser.accepts( "output_xoffset", "produced records include xoffset, option \"output_commit_info\" must be enabled; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_nulls", "produced records include fields with NULL values [true|false]. default: true" ).withOptionalArg();
		parser.accepts( "output_server_id", "produced records include server_id; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_thread_id", "produced records include thread_id; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_row_query", "produced records include query, binlog option \"binlog_rows_query_log_events\" must be enabled; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_ddl", "produce DDL records to ddl_kafka_topic [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "ddl_kafka_topic", "optionally provide an alternate topic to push DDL records to. default: kafka_topic" ).withRequiredArg();
		parser.accepts("secret_key", "The secret key for the AES encryption" ).withRequiredArg();
		parser.accepts("encrypt", "encryption mode: [none|data|all]. default: none" ).withRequiredArg();

		parser.accepts( "__separator_5" );

		parser.accepts( "bootstrapper", "bootstrapper type: async|sync|none. default: async" ).withRequiredArg();

		parser.accepts( "__separator_6" );

		parser.accepts( "replica_server_id", "server_id that maxwell reports to the master.  See docs for full explanation. ").withRequiredArg();
		parser.accepts( "client_id", "unique identifier for this maxwell replicator" ).withRequiredArg();
		parser.accepts( "schema_database", "database name for maxwell state (schema and binlog position)" ).withRequiredArg();
		parser.accepts( "max_schemas", "[deprecated]" ).withRequiredArg();
		parser.accepts( "init_position", "initial binlog position, given as BINLOG_FILE:POSITION[:HEARTBEAT]" ).withRequiredArg();
		parser.accepts( "replay", "replay mode, don't store any information to the server" ).withOptionalArg();
		parser.accepts( "master_recovery", "(experimental) enable master position recovery code" ).withOptionalArg();
		parser.accepts( "gtid_mode", "(experimental) enable gtid mode" ).withOptionalArg();
		parser.accepts( "ignore_producer_error", "Maxwell will be terminated on kafka/kinesis errors when false. Otherwise, those producer errors are only logged. Default to true" ).withOptionalArg();

		parser.accepts( "__separator_7" );

		parser.accepts( "include_dbs", "include these databases, formatted as include_dbs=db1,db2" ).withRequiredArg();
		parser.accepts( "exclude_dbs", "exclude these databases, formatted as exclude_dbs=db1,db2" ).withRequiredArg();
		parser.accepts( "include_tables", "include these tables, formatted as include_tables=db1,db2" ).withRequiredArg();
		parser.accepts( "exclude_tables", "exclude these tables, formatted as exclude_tables=tb1,tb2" ).withRequiredArg();
		parser.accepts( "exclude_columns", "exclude these columns, formatted as exclude_columns=col1,col2" ).withRequiredArg();
		parser.accepts( "blacklist_dbs", "ignore data AND schema changes to these databases, formatted as blacklist_dbs=db1,db2. See the docs for details before setting this!" ).withRequiredArg();
		parser.accepts( "blacklist_tables", "ignore data AND schema changes to these tables, formatted as blacklist_tables=tb1,tb2. See the docs for details before setting this!" ).withRequiredArg();
		parser.accepts( "include_column_values", "include only rows with these values formatted as include_column_values=C=x,D=y" ).withRequiredArg();

		parser.accepts( "__separator_8" );

		parser.accepts( "rabbitmq_user", "Username of Rabbitmq connection. Default is guest" ).withRequiredArg();
		parser.accepts( "rabbitmq_pass", "Password of Rabbitmq connection. Default is guest" ).withRequiredArg();
		parser.accepts( "rabbitmq_host", "Host of Rabbitmq machine" ).withRequiredArg();
		parser.accepts( "rabbitmq_port", "Port of Rabbitmq machine" ).withRequiredArg();
		parser.accepts( "rabbitmq_virtual_host", "Virtual Host of Rabbitmq" ).withRequiredArg();
		parser.accepts( "rabbitmq_exchange", "Name of exchange for rabbitmq publisher" ).withRequiredArg();
		parser.accepts( "rabbitmq_exchange_type", "Exchange type for rabbitmq" ).withRequiredArg();
		parser.accepts( "rabbitmq_exchange_durable", "Exchange durability. Default is disabled" ).withOptionalArg();
		parser.accepts( "rabbitmq_exchange_autodelete", "If set, the exchange is deleted when all queues have finished using it. Defaults to false" ).withOptionalArg();
		parser.accepts( "rabbitmq_routing_key_template", "A string template for the routing key, '%db%' and '%table%' will be substituted. Default is '%db%.%table%'." ).withRequiredArg();
		parser.accepts( "rabbitmq_message_persistent", "Message persistence. Defaults to false" ).withOptionalArg();
		parser.accepts( "rabbitmq_declare_exchange", "Should declare the exchange for rabbitmq publisher. Defaults to true" ).withOptionalArg();

		parser.accepts( "__separator_9" );

		parser.accepts( "redis_host", "Host of Redis server" ).withRequiredArg();
		parser.accepts( "redis_port", "Port of Redis server" ).withRequiredArg();
		parser.accepts( "redis_auth", "Authentication key for a password-protected Redis server" ).withRequiredArg();
		parser.accepts( "redis_database", "Database of Redis server" ).withRequiredArg();
		parser.accepts( "redis_pub_channel", "Redis Pub/Sub channel for publishing records" ).withRequiredArg();
		parser.accepts( "redis_list_key", "Redis LPUSH List Key for adding to a queue" ).withRequiredArg();
		parser.accepts( "redis_type", "[pubsub|lpush] Selects either Redis Pub/Sub or LPUSH. Defaults to 'pubsub'" ).withRequiredArg();

		parser.accepts( "__separator_10" );

		parser.accepts( "metrics_prefix", "the prefix maxwell will apply to all metrics" ).withRequiredArg();
		parser.accepts( "metrics_type", "how maxwell metrics will be reported, at least one of slf4j|jmx|http|datadog" ).withRequiredArg();
		parser.accepts( "metrics_slf4j_interval", "the frequency metrics are emitted to the log, in seconds, when slf4j reporting is configured" ).withRequiredArg();
		parser.accepts( "metrics_http_port", "[deprecated]" ).withRequiredArg();
		parser.accepts( "http_port", "the port the server will bind to when http reporting is configured" ).withRequiredArg();
		parser.accepts( "http_path_prefix", "the http path prefix when metrics_type includes http or diagnostic is enabled, default /" ).withRequiredArg();
		parser.accepts( "http_bind_address", "the ip address the server will bind to when http reporting is configured" ).withRequiredArg();
		parser.accepts( "metrics_datadog_type", "when metrics_type includes datadog this is the way metrics will be reported, one of udp|http" ).withRequiredArg();
		parser.accepts( "metrics_datadog_tags", "datadog tags that should be supplied, e.g. tag1:value1,tag2:value2" ).withRequiredArg();
		parser.accepts( "metrics_datadog_interval", "the frequency metrics are pushed to datadog, in seconds" ).withRequiredArg();
		parser.accepts( "metrics_datadog_apikey", "the datadog api key to use when metrics_datadog_type = http" ).withRequiredArg();
		parser.accepts( "metrics_datadog_host", "the host to publish metrics to when metrics_datadog_type = udp" ).withRequiredArg();
		parser.accepts( "metrics_datadog_port", "the port to publish metrics to when metrics_datadog_type = udp" ).withRequiredArg();
		parser.accepts( "http_diagnostic", "enable http diagnostic endpoint: true|false. default: false" ).withOptionalArg();
		parser.accepts( "http_diagnostic_timeout", "the http diagnostic response timeout in ms when http_diagnostic=true. default: 10000" ).withRequiredArg();
		parser.accepts( "metrics_jvm", "enable jvm metrics: true|false. default: false" ).withRequiredArg();

		parser.accepts( "__separator_11" );

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

}
