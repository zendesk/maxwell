package com.zendesk.maxwell.benchmark;

import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellTestSupport;
import com.zendesk.maxwell.MysqlIsolatedServer;
import com.zendesk.maxwell.replication.Position;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import me.tongfei.progressbar.ProgressBar;

import java.sql.*;
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
		MysqlIsolatedServer server = MaxwellTestSupport.setupServer("--no-clean");
		MaxwellTestSupport.setupSchema(server, false);

		// generate 1 row of data before we capture position so that we can use the schema.
		generateData(server.getConnection(), 1);

		Position initPosition = Position.capture(server.getConnection(), false);
		generateData(server.getConnection(), nRows);
		System.out.println("generated data.  you may now run:");
		System.out.println("bin/maxwell-benchmark --input=" +  server.path + " --init_position=" + initPosition.toCommandline());
	}

	private static void benchmark(String path, long skipRows, String args[]) throws Exception {
		MaxwellConfig config = new MaxwellConfig(args);
		MysqlIsolatedServer server = MaxwellTestSupport.setupServer("--no-clean --reuse=" + path);


		config.maxwellMysql.host = "127.0.0.1";
		config.maxwellMysql.port = server.getPort();
		config.maxwellMysql.user = "root";
		config.maxwellMysql.password = "";
		config.replicationMysql = config.schemaMysql = config.maxwellMysql;
		config.producerFactory = new BenchmarkProducerFactory(skipRows);

		Maxwell m = new Maxwell(config);
		m.run();
	}

	private static OptionParser buildOptionParser() {
		final OptionParser parser = new OptionParser();
		parser.accepts("generate", "generate this many rows of benchmark data").withRequiredArg();
		parser.accepts("input", "run benchmark using this mysql data-path").withRequiredArg();
		parser.accepts("skip", "warm-up by processing this many rows before profiling").withRequiredArg();
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

			long skipNRows = 0;
			if ( options.has("skip") )
				skipNRows = Long.parseLong((String) options.valueOf("skip"));

			benchmark((String) options.valueOf("input"), skipNRows, maxwellArgs);
		} else {
			p.printHelpOn(System.out);
			System.exit(1);
		}
	}
}
