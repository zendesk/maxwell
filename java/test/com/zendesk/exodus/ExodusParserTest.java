package com.zendesk.exodus;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.code.or.binlog.BinlogEventV4;
import com.zendesk.exodus.schema.Schema;
import com.zendesk.exodus.schema.SchemaCapturer;

import static org.hamcrest.CoreMatchers.*;

public class ExodusParserTest extends AbstractMaxwellTest {
	@Test
	public void testGetEvent() throws Exception {
		List<ExodusAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		list = getRowsForSQL(null, input);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).toSQL(), is("REPLACE INTO `minimal` (`id`, `account_id`, `text_field`) VALUES (1,1,'hello')"));
	}

	@Test
	public void testRowFilter() throws Exception {
		List<ExodusAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id = 1, text_field='hello'",
						  "insert into minimal set account_id = 2, text_field='goodbye'"};

		list = getRowsForSQL(null, input);
		assertThat(list.size(), is(2));

		ExodusFilter filter = new ExodusFilter();
		filter.addRowConstraint("account_id", 2);

		list = getRowsForSQL(filter, input);
		assertThat(list.size(), is(1));

		System.out.println(list.get(0).toSQL());

		Map<String, Object> jsonMap = list.get(0).jsonMaps().get(0);

		assertThat((Long) jsonMap.get("account_id"), is(2L));
	}

	@Test
	public void testRowFilterOnNonExistantFields() throws Exception {
		List<ExodusAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id = 1, text_field='hello'",
						  "insert into minimal set account_id = 2, text_field='goodbye'"};

		ExodusFilter filter = new ExodusFilter();
		filter.addRowConstraint("piggypiggy", 2);

		list = getRowsForSQL(filter, input);
		assertThat(list.size(), is(0));
	}

	static String createDBs[] = {
		"CREATE database foo",
		"CREATE table foo.bars ( id int(11) auto_increment not null, something text, primary key (id) )",
	};

	static String insertSQL[] = {
		"INSERT into foo.bars set something = 'hi'",
		"INSERT into shard_1.minimal set account_id = 2, text_field='sigh'"
	};

	@Test
	public void testIncludeDB() throws Exception {
		ExodusAbstractRowsEvent e;
		List<ExodusAbstractRowsEvent> list;

		ExodusFilter filter = new ExodusFilter();

		list = getRowsForSQL(filter, insertSQL, createDBs);
		assertThat(list.size(), is(2));
		e = list.get(0);
		assertThat(e.getTable().getName(), is("bars"));

		filter.includeDatabase("shard_1");
		list = getRowsForSQL(filter, insertSQL);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).getTable().getName(), is("minimal"));
	}
}
