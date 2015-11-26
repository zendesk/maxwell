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
		List<MaxwellAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		list = getRowsForSQL(null, input);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).toSQL(), is("REPLACE INTO `minimal` (`id`, `account_id`, `text_field`) VALUES (1,1,'hello')"));
	}

	@Test
	public void testPrimaryKeyStrings() throws Exception {
		List<MaxwellAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		String expectedJSON = "{\"database\":\"shard_1\",\"table\":\"minimal\",\"pk.id\":1,\"pk.text_field\":\"hello\"}";
		list = getRowsForSQL(null, input);
		assertThat(list.size(), is(1));
		assertThat(StringUtils.join(list.get(0).getPKStrings(), ""), is(expectedJSON));
	}

	@Test
	public void testRowFilter() throws Exception {
		List<MaxwellAbstractRowsEvent> list;
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

		RowMap jsonMap = list.get(0).jsonMaps().get(0);

		assertThat((Long) jsonMap.getData("account_id"), is(2000L));
		assertThat((String) jsonMap.getData("text_field"), is("goodbye"));
	}

	@Test
	public void testRowFilterOnNonExistentFields() throws Exception {
		List<MaxwellAbstractRowsEvent> list;
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
		MaxwellAbstractRowsEvent e;
		List<MaxwellAbstractRowsEvent> list;

		MaxwellFilter filter = new MaxwellFilter();

		list = getRowsForSQL(filter, insertSQL, createDBs);
		assertThat(list.size(), is(2));
		e = list.get(0);
		assertThat(e.getTable().getName(), is("bars"));

		filter.includeDatabase("shard_1");
		list = getRowsForSQL(filter, insertSQL);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).getTable().getName(), is("minimal"));
	}

	@Test
	public void testExcludeDB() throws Exception {
		MaxwellAbstractRowsEvent e;
		List<MaxwellAbstractRowsEvent> list;

		MaxwellFilter filter = new MaxwellFilter();
		filter.excludeDatabase("shard_1");
		list = getRowsForSQL(filter, insertSQL, createDBs);
		assertThat(list.size(), is(1));

		e = list.get(0);
		assertThat(e.getTable().getName(), is("bars"));
	}

	@Test
	public void testIncludeTable() throws Exception {
		MaxwellAbstractRowsEvent e;
		List<MaxwellAbstractRowsEvent> list;

		MaxwellFilter filter = new MaxwellFilter();
		filter.includeTable("minimal");

		list = getRowsForSQL(filter, insertSQL, createDBs);

		assertThat(list.size(), is(1));

		e = list.get(0);
		assertThat(e.getTable().getName(), is("minimal"));
	}

	@Test
	public void testExcludeTable() throws Exception {
		MaxwellAbstractRowsEvent e;
		List<MaxwellAbstractRowsEvent> list;

		MaxwellFilter filter = new MaxwellFilter();
		filter.excludeTable("minimal");

		list = getRowsForSQL(filter, insertSQL, createDBs);

		assertThat(list.size(), is(1));

		e = list.get(0);
		assertThat(e.getTable().getName(), is("bars"));
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
		MaxwellAbstractRowsEvent e;
		List<MaxwellAbstractRowsEvent> list;

		list = getRowsForSQL(null, testAlterSQL, null);

		e = list.get(0);
		assertThat(e.getTable().getName(), is("minimal"));
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
		List<MaxwellAbstractRowsEvent> list;

		try {
			server.getConnection().setAutoCommit(false);
			list = getRowsForSQL(null, testTransactions, null);

			ArrayList<Map<String, Object>> objects = new ArrayList<>();
			for (MaxwellAbstractRowsEvent e : list) {
				for ( String s : e.toJSONStrings() ) {
					Map<String, Object> m = new ObjectMapper().readValue(s, MAP_STRING_OBJECT_REF);
					assertTrue(m.containsKey("xid"));
					objects.add(m);
				}
			}
			assertEquals(4, objects.size());

			assertEquals(objects.get(0).get("xid"), objects.get(1).get("xid"));
			assertFalse(objects.get(0).containsKey("commit"));
			assertTrue(objects.get(1).containsKey("commit"));

			assertFalse(objects.get(2).containsKey("commit"));
			assertTrue(objects.get(3).containsKey("commit"));
		} finally {
			server.getConnection().setAutoCommit(true);
		}
	}


	private void runJSONTest(List<String> sql, List<Map<String, Object>> assertJSON) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		List<Map<String, Object>> eventJSON = new ArrayList<>();
		List<Map<String, Object>> matched = new ArrayList<>();
		List<MaxwellAbstractRowsEvent> events = getRowsForSQL(null, sql.toArray(new String[sql.size()]));

		for ( MaxwellAbstractRowsEvent e : events ) {
			for ( String s : e.toJSONStrings() ) {
				Map<String, Object> r = mapper.readValue(s, MAP_STRING_OBJECT_REF);
				r.remove("ts");
				r.remove("xid");
				r.remove("commit");

				eventJSON.add(r);

				for ( Map<String, Object> b : assertJSON ) {
					if ( r.equals(b) )
						matched.add(b);
				}
			}
		}

		for ( Map j : matched ) {
			assertJSON.remove(j);
		}

		if ( assertJSON.size() > 0 ) {
			String msg = "Did not find: \n" +
						 StringUtils.join(assertJSON.iterator(), "\n") +
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
}
