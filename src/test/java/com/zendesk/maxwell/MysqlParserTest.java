package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;
import org.junit.Test;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent.RowMap;

public class MysqlParserTest extends AbstractMaxwellTest {
	@Test
	public void testGetEvent() throws Exception {
		List<MaxwellAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		list = getRowsForSQL(null, input);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).toSQL(), is("REPLACE INTO `minimal` (`id`, `account_id`, `text_field`) VALUES (1,1,'hello')"));
	}

	@Test
	public void testRowFilter() throws Exception {
		List<MaxwellAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id = 1, text_field='hello'",
						  "insert into minimal set account_id = 2, text_field='goodbye'"};

		list = getRowsForSQL(null, input);
		assertThat(list.size(), is(2));

		MaxwellFilter filter = new MaxwellFilter();
		filter.addRowConstraint("account_id", 2);

		list = getRowsForSQL(filter, input);
		assertThat(list.size(), is(1));

		RowMap jsonMap = list.get(0).jsonMap();

		assertThat((Long) jsonMap.data().get(0).get("account_id"), is(2L));
		assertThat((String) jsonMap.data().get(0).get("text_field"), is("goodbye"));
	}

	@Test
	public void testRowFilterOnNonExistantFields() throws Exception {
		List<MaxwellAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id = 1, text_field='hello'",
						  "insert into minimal set account_id = 2, text_field='goodbye'"};

		MaxwellFilter filter = new MaxwellFilter();
		filter.addRowConstraint("piggypiggy", 2);

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

	private void runJSONTest(List<String> sql, List<JSONObject> assertJSON) throws Exception {
		List<JSONObject> eventJSON = new ArrayList<>();
		List<JSONObject> matched = new ArrayList<>();
		List<MaxwellAbstractRowsEvent> events = getRowsForSQL(null, sql.toArray(new String[0]));

		for ( MaxwellAbstractRowsEvent e : events ) {
			JSONObject a = e.toJSONObject();

			eventJSON.add(a);

			for ( JSONObject b : assertJSON ) {
				if ( JSONCompare.compare(a.toString(), b.toString()) )
					matched.add(b);
			}
		}

		for ( JSONObject j : matched ) {
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
		ArrayList<JSONObject> jsonAsserts = new ArrayList<>();
		ArrayList<String> inputSQL  = new ArrayList<>();
		BufferedReader reader = new BufferedReader(new FileReader(file));

		while ( reader.ready() ) {
			String line = reader.readLine();
			if ( line.matches("^\\s*$")) {
				continue;
			} else if ( line.matches("^\\s*\\-\\>\\s*\\{.*") ) {
				line = line.replaceAll("^\\s*\\-\\>\\s*", "");
				jsonAsserts.add(new JSONObject(line));
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
		runJSONTestFile(getSQLDir() + "/json/test_zero_created_at");
	}
}
