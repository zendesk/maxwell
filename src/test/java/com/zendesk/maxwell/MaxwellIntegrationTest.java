package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class MaxwellIntegrationTest extends AbstractMaxwellTest {
	public static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_REF = new TypeReference<Map<String, Object>>() {};

	@Test
	public void testGetEvent() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id = 1, text_field='hello'"};
		list = getRowsForSQL(null, input);
		assertThat(list.size(), is(1));
	}

	@Test
	public void testPrimaryKeyStrings() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		String expectedJSON = "{\"database\":\"shard_1\",\"table\":\"minimal\",\"pk.id\":1,\"pk.text_field\":\"hello\"}";
		list = getRowsForSQL(null, input);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).pkToJson(), is(expectedJSON));
	}

	@Test
	public void testRowFilter() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id = 1000, text_field='hello'",
						  "insert into minimal set account_id = 2000, text_field='goodbye'"};

		list = getRowsForSQL(null, input);
		assertThat(list.size(), is(2));

		MaxwellFilter filter = new MaxwellFilter();

		@SuppressWarnings("UnnecessaryBoxing")
		Integer filterValue = new Integer(2000); // make sure we're using a different instance of the filter value

		filter.addRowConstraint("account_id", filterValue);

		list = getRowsForSQL(filter, input);
		assertThat(list.size(), is(1));

		RowMap jsonMap = list.get(0);

		assertThat((Long) jsonMap.getData("account_id"), is(2000L));
		assertThat((String) jsonMap.getData("text_field"), is("goodbye"));
	}

	@Test
	public void testRowFilterOnNonExistentFields() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id = 1000, text_field='hello'",
						  "insert into minimal set account_id = 2000, text_field='goodbye'"};

		MaxwellFilter filter = new MaxwellFilter();
		filter.addRowConstraint("piggypiggy", 2000);

		list = getRowsForSQL(filter, input);
		assertThat(list.size(), is(0));
	}

	static String createDBs[] = {
		"CREATE database if not exists foo",
		"CREATE table if not exists foo.bars ( id int(11) auto_increment not null, something text, primary key (id) )",
	};

	static String insertSQL[] = {
		"INSERT into foo.bars set something = 'hi'",
		"INSERT into shard_1.minimal set account_id = 2, text_field='sigh'"
	};

	@Test
	public void testIncludeDB() throws Exception {
		List<RowMap> list;
		RowMap r;

		MaxwellFilter filter = new MaxwellFilter();

		list = getRowsForSQL(filter, insertSQL, createDBs);
		assertThat(list.size(), is(2));

		r = list.get(0);
		assertThat(r.getTable(), is("bars"));

		filter.includeDatabase("shard_1");
		list = getRowsForSQL(filter, insertSQL);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).getTable(), is("minimal"));
	}

	@Test
	public void testExcludeDB() throws Exception {
		List<RowMap> list;

		MaxwellFilter filter = new MaxwellFilter();
		filter.excludeDatabase("shard_1");
		list = getRowsForSQL(filter, insertSQL, createDBs);
		assertThat(list.size(), is(1));

		assertThat(list.get(0).getTable(), is("bars"));
	}

	@Test
	public void testIncludeTable() throws Exception {
		List<RowMap> list;

		MaxwellFilter filter = new MaxwellFilter();
		filter.includeTable("minimal");

		list = getRowsForSQL(filter, insertSQL, createDBs);

		assertThat(list.size(), is(1));

		assertThat(list.get(0).getTable(), is("minimal"));
	}

	@Test
	public void testExcludeTable() throws Exception {
		List<RowMap> list;

		MaxwellFilter filter = new MaxwellFilter();
		filter.excludeTable("minimal");

		list = getRowsForSQL(filter, insertSQL, createDBs);

		assertThat(list.size(), is(1));

		assertThat(list.get(0).getTable(), is("bars"));
	}

	String testAlterSQL[] = {
			"insert into minimal set account_id = 1, text_field='hello'",
			"ALTER table minimal drop column text_field",
			"insert into minimal set account_id = 2",
			"ALTER table minimal add column new_text_field varchar(255)",
			"insert into minimal set account_id = 2, new_text_field='hihihi'",
	};

	@Test
	public void testAlterTable() throws Exception {
		List<RowMap> list;

		list = getRowsForSQL(null, testAlterSQL, null);

		assertThat(list.get(0).getTable(), is("minimal"));
	}

	@Test
	public void testMyISAMCommit() throws Exception {
		String sql[] = {
				"CREATE TABLE myisam_test ( id int ) engine=myisam",
				"insert into myisam_test (id) values (1), (2), (3)"

		};

		List<RowMap> list = getRowsForSQL(null, sql, null);
		assertThat(list.size(), is(3));
		assertThat(list.get(2).isTXCommit(), is(true));
	}

	String testTransactions[] = {
			"BEGIN",
			"insert into minimal set account_id = 1, text_field = 's'",
			"insert into minimal set account_id = 2, text_field = 's'",
			"COMMIT",
			"BEGIN",
			"insert into minimal (account_id, text_field) values (3, 's'), (4, 's')",
			"COMMIT"
	};

	@Test
	public void testTransactionID() throws Exception {
		List<RowMap> list;

		try {
			server.getConnection().setAutoCommit(false);
			list = getRowsForSQL(null, testTransactions, null);

			assertEquals(4, list.size());
			for ( RowMap r : list ) {
				assertNotNull(r.getXid());
			}

			assertEquals(list.get(0).getXid(), list.get(1).getXid());
			assertFalse(list.get(0).isTXCommit());
			assertTrue(list.get(1).isTXCommit());

			assertFalse(list.get(2).isTXCommit());
			assertTrue(list.get(3).isTXCommit());
		} finally {
			server.getConnection().setAutoCommit(true);
		}
	}

	@Test
	public void testRunMinimalBinlog() throws Exception {
		if ( server.getVersion().equals("5.5") )
			return;

		try {
			server.getConnection().createStatement().execute("set global binlog_row_image='minimal'");
			server.resetConnection(); // only new connections pick up the binlog setting

			runJSONTestFile(getSQLDir() + "/json/test_minimal");
		} finally {
			server.getConnection().createStatement().execute("set global binlog_row_image='full'");
			server.resetConnection();
		}
	}


	private void runJSONTest(List<String> sql, List<Map<String, Object>> expectedJSON) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		List<Map<String, Object>> eventJSON = new ArrayList<>();
		List<Map<String, Object>> matched = new ArrayList<>();
		List<RowMap> rows = getRowsForSQL(null, sql.toArray(new String[sql.size()]));

		for ( RowMap r : rows ) {
			String s = r.toJSON();

			Map<String, Object> outputMap = mapper.readValue(s, MAP_STRING_OBJECT_REF);

			outputMap.remove("ts");
			outputMap.remove("xid");
			outputMap.remove("commit");

			eventJSON.add(outputMap);

			for ( Map<String, Object> b : expectedJSON ) {
				if ( outputMap.equals(b) )
					matched.add(b);
			}
		}

		for ( Map j : matched ) {
			expectedJSON.remove(j);
		}

		if ( expectedJSON.size() > 0 ) {
			String msg = "Did not find: \n" +
						 StringUtils.join(expectedJSON.iterator(), "\n") +
						 "\n\n in : " +
						 StringUtils.join(eventJSON.iterator(), "\n");
			assertThat(msg, false, is(true));

		}
	}

	private void runJSONTestFile(String fname) throws Exception {
		File file = new File(fname);
		ArrayList<Map<String, Object>> jsonAsserts = new ArrayList<>();
		ArrayList<String> inputSQL  = new ArrayList<>();
		BufferedReader reader = new BufferedReader(new FileReader(file));
		ObjectMapper mapper = new ObjectMapper();

		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

		while ( reader.ready() ) {
			String line = reader.readLine();
			if ( line.matches("^\\s*$")) {
				continue;
			}

			if ( line.matches("^\\s*\\->\\s*\\{.*") ) {
				line = line.replaceAll("^\\s*\\->\\s*", "");

				jsonAsserts.add(mapper.<Map<String, Object>>readValue(line, MAP_STRING_OBJECT_REF));
				System.out.println("added json assert: " + line);
			} else {
				inputSQL.add(line);
				System.out.println("added sql statement: " + line);
			}
		}
		reader.close();

	    runJSONTest(inputSQL, jsonAsserts);
	}

	@Test
	public void testRunMainJSONTest() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_1j");
	}

	@Test
	public void testCreateLikeJSON() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_create_like");
	}

	@Test
	public void testCreateSelectJSON() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_create_select");
	}

	@Test
	public void testEnumJSON() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_enum");
	}

	@Test
	public void testLatin1JSON() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_latin1");
	}

	@Test
	public void testSetJSON() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_set");
	}

	@Test
	public void testZeroCreatedAtJSON() throws Exception {
		if ( server.getVersion().equals("5.5") ) // 5.6 not yet supported for this test
			runJSONTestFile(getSQLDir() + "/json/test_zero_created_at");
	}

	@Test
	public void testCaseSensitivity() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_case_insensitive");
	}

	@Test
	public void testBlob() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_blob");
	}

	@Test
	public void testBit() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_bit");
	}

	@Test
	public void testBignum() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_bignum");
	}

	@Test
	public void testTime() throws Exception {
		if ( server.getVersion().equals("5.6") )
			runJSONTestFile(getSQLDir() + "/json/test_time");
	}

	@Test
	public void testUCS2() throws Exception {
		runJSONTestFile(getSQLDir() + "/json/test_ucs2");
	}
}
