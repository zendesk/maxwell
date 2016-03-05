package com.zendesk.maxwell;

import java.util.*;
import org.junit.*;


public class MaxwellTestWithIsolatedServer {
	protected static MysqlIsolatedServer server;

	@BeforeClass
	public static void setupTest() throws Exception {
		server = MaxwellTestSupport.setupServer();
	}

	@Before
	public void setupSchema() throws Exception {
		MaxwellTestSupport.setupSchema(server);
	}

	protected List<RowMap> getRowsForSQL(MaxwellFilter filter, String[] input) throws Exception {
		return MaxwellTestSupport.getRowsForSQL(server, filter, input);
	}

	protected List<RowMap> getRowsForSQL(MaxwellFilter filter, String[] input, String[] before) throws Exception {
		return MaxwellTestSupport.getRowsForSQL(server, filter, input, before);
	}

	protected List<RowMap> getRowsForSQL(String[] input) throws Exception {
		return MaxwellTestSupport.getRowsForSQL(server, null, input, null);
	}
	protected void runJSON(String filename) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename);
	}

	protected MaxwellContext buildContext() {
		return MaxwellTestSupport.buildContext(server.getPort(), null);
	}

	protected MaxwellContext buildContext(BinlogPosition p) {
		return MaxwellTestSupport.buildContext(server.getPort(), p);
	}
}
