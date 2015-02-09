package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.Test;

public class MaxwellParserTest extends AbstractMaxwellTest {
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

		MaxwellFilter filter = new ExodusFilter();
		filter.addRowConstraint("account_id", 2);

		list = getRowsForSQL(filter, input);
		assertThat(list.size(), is(1));

		Map<String, Object> jsonMap = list.get(0).jsonMaps().get(0);

		assertThat((Long) jsonMap.get("account_id"), is(2L));
		assertThat((String) jsonMap.get("text_field"), is("goodbye"));
	}

	@Test
	public void testRowFilterOnNonExistantFields() throws Exception {
		List<MaxwellAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id = 1, text_field='hello'",
						  "insert into minimal set account_id = 2, text_field='goodbye'"};

		MaxwellFilter filter = new ExodusFilter();
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

		MaxwellFilter filter = new ExodusFilter();

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

		MaxwellFilter filter = new ExodusFilter();
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

		MaxwellFilter filter = new ExodusFilter();
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

		MaxwellFilter filter = new ExodusFilter();
		filter.excludeTable("minimal");

		list = getRowsForSQL(filter, insertSQL, createDBs);

		assertThat(list.size(), is(1));

		e = list.get(0);
		assertThat(e.getTable().getName(), is("bars"));
	}


}
