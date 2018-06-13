package com.zendesk.maxwell.standalone.benchmark;

import com.zendesk.maxwell.core.LauncherException;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.config.MaxwellMysqlConfig;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.standalone.MaxwellStandaloneRuntime;
import com.zendesk.maxwell.core.util.test.mysql.MysqlIsolatedServer;
import com.zendesk.maxwell.standalone.SpringLauncher;
import com.zendesk.maxwell.core.util.test.mysql.MysqlIsolatedServerSupport;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import me.tongfei.progressbar.ProgressBar;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class MaxwellBenchmark {
	/*
	CREATE TABLE `sharded` (
		`id` bigint(20) NOT NULL AUTO_INCREMENT,
  		`account_id` int(11) UNSIGNED NOT NULL,
  		`nice_id` int(11) NOT NULL,
  		`status_id` tinyint NOT NULL default 2,
		`date_field` datetime,
		`text_field` text,
		`latin1_field` varchar(96) CHARACTER SET latin1 NOT NULL DEFAULT '',
		`utf8_field` varchar(96) CHARACTER SET utf8 NOT NULL DEFAULT '',
		`float_field` float(5,2),
  		`timestamp_field` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  		`timestamp2_field` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  		`datetime2_field` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  		-- Can't have a default time
		-- See http://dev.mysql.com/doc/refman/5.6/en/timestamp-initialization.html
		`time2_field` time(6),
  		`decimal_field` decimal(12,7),
  */

	static Random rand = new Random();

	private static void generateTX(Connection cx, int rowsInTX) throws SQLException {
		PreparedStatement ps = cx.prepareStatement(
			"INSERT INTO shard_1.sharded (id, account_id, nice_id, status_id, date_field, text_field) VALUES(?, ?, ?, ?, ?, ?)"
		);

		for ( int i = 0; i < rowsInTX; i++) {
			ps.setObject(1, null);
			ps.setInt(2, rand.nextInt(1000000));
			ps.setInt(3, rand.nextInt(1000000));
			ps.setInt(4, rand.nextInt(128));
			ps.setDate(5, new Date(rand.nextInt()));
			ps.setString(6, UUID.randomUUID().toString());
			ps.addBatch();
			ps.clearParameters();
		}
		ps.executeBatch();
		ps.close();
	}

	private static void generateData(Connection cx, int nRows) throws SQLException {
		ProgressBar pb = new ProgressBar("generating rows", nRows);
		pb.start();
		while ( nRows > 0 ) {
			int toGenerate = rand.nextInt(50) + 1;
			generateTX(cx, toGenerate);
			nRows -= toGenerate;
			pb.stepBy(toGenerate);
		}
		pb.stop();
	}

	private static void generate(int nRows) throws Exception {
		SpringLauncher.launch((ctx) -> {
			try {
				MysqlIsolatedServerSupport mysqlIsolatedServerSupport = ctx.getBean(MysqlIsolatedServerSupport.class);
				MysqlIsolatedServer server = mysqlIsolatedServerSupport.setupServer("--no-clean");
				mysqlIsolatedServerSupport.setupSchema(server, false);

				// generate 1 row of data before we capture position so that we can use the schema.
				generateData(server.getConnection(), 1);

				Position initPosition = Position.capture(server.getConnection(), false);
				generateData(server.getConnection(), nRows);
				System.out.println("generated data.  you may now run:");
				System.out.println("bin/maxwell-benchmark --input=" + server.path + " --init_position=" + initPosition.toCommandline());
			}catch (Exception e){
				throw new LauncherException("Failed to generate benchmark environment", e);
			}
		});

	}

	private static void benchmark(final String path, final String args[]) throws Exception {
		SpringLauncher.launch((ctx) -> {
			try {
				MysqlIsolatedServerSupport mysqlIsolatedServerSupport = ctx.getBean(MysqlIsolatedServerSupport.class);
				MaxwellStandaloneRuntime launcher = ctx.getBean(MaxwellStandaloneRuntime.class);
				MysqlIsolatedServer server = mysqlIsolatedServerSupport.setupServer("--no-clean --reuse=" + path);

				Properties configuration = new Properties();
				appendMySQLSetting(configuration,"", server);
				appendMySQLSetting(configuration, MaxwellMysqlConfig.CONFIGURATION_OPTION_PREFIX_REPLICATION, server);
				appendMySQLSetting(configuration, MaxwellMysqlConfig.CONFIGURATION_OPTION_PREFIX_SCHEMA, server);
				configuration.put(MaxwellConfig.CONFIGURATION_OPTION_CUSTOM_PRODUCER_FACTORY, BenchmarkProducerFactory.class.getCanonicalName());

				launcher.runMaxwell(configuration);
			} catch (Exception e) {
				throw new LauncherException("Failed to setup mysql test server for benchmark", e);
			}
		});
	}

	private static void appendMySQLSetting(Properties properties, String prefix, MysqlIsolatedServer server) {
		properties.put(prefix+MaxwellMysqlConfig.CONFIGURATION_OPTION_HOST, "127.0.0.1");
		properties.put(prefix+MaxwellMysqlConfig.CONFIGURATION_OPTION_PORT, ""+server.getPort());
		properties.put(prefix+MaxwellMysqlConfig.CONFIGURATION_OPTION_USER, "root");
		properties.put(prefix+MaxwellMysqlConfig.CONFIGURATION_OPTION_PASSWORD, "");
	}

	private static OptionParser buildOptionParser() {
		final OptionParser parser = new OptionParser();
		parser.accepts("generate", "generate this many rows of benchmark data").withRequiredArg();
		parser.accepts("input", "run benchmark using this mysql data-path").withRequiredArg();
		parser.allowsUnrecognizedOptions();
		parser.formatHelpWith(new BuiltinHelpFormatter(120, 5));
		return parser;
	}


	public static void main(String args[]) throws Exception {

		OptionParser p = buildOptionParser();
		OptionSet options = p.parse(args);

		if ( options.has("generate") ) {
			generate(Integer.parseInt((String) options.valueOf("generate")));
		} else if ( options.has("input") ) {
			String maxwellArgs[] = new String[options.nonOptionArguments().size()];
			int i = 0;
			for(Object o : options.nonOptionArguments()) {
				maxwellArgs[i++] = o.toString();
			}

			benchmark((String) options.valueOf("input"), maxwellArgs);
		} else {
			p.printHelpOn(System.out);
			System.exit(1);
		}
	}
}
