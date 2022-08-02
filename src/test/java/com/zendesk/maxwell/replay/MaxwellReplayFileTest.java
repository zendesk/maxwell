package com.zendesk.maxwell.replay;

import com.zendesk.maxwell.MaxwellTestSupport;
import org.junit.Test;

/**
 * @author udyr@shlaji.com
 */
public class MaxwellReplayFileTest {

	@Test
	public void testReplay() {
		String binlogPath = MaxwellTestSupport.getSQLDir() + "replay/binlog.000004";
		String[] args = new String[]{
				"--host=127.0.0.1",
				"--port=3306",
				"--user=root",
				"--password=root",
				"--schema_database=maxwell",
				"--producer=stdout",
				"--output_ddl=true",
				"--output_server_id=true",
				"--output_thread_id=true",
				"--output_schema_id=true",
				"--output_row_query=true",
				"--output_primary_keys=false",
				"--output_primary_key_columns=false",
				"--output_push_timestamp=true",
				"--filter=exclude:test.*",
				"--replay_binlog=" + binlogPath
		};
		MaxwellReplayFile.main(args);
	}
}
