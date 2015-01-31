package com.zendesk.exodus;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
		list = getRowsForSQL(input, null);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).toSql(), is("REPLACE INTO `minimal` (`id`, `account_id`, `text_field`) VALUES (1,1,'hello')"));
	}

	@Test
	public void testRowFilter() throws Exception {
		List<ExodusAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id = 1, text_field='hello'",
						  "insert into minimal set account_id = 2, text_field='goodbye'"};

		list = getRowsForSQL(input, null);
		assertThat(list.size(), is(2));

		ExodusFilter filter = new ExodusFilter();
		filter.addRowConstraint("account_id", 2);

		list = getRowsForSQL(input, filter);
		assertThat(list.size(), is(1));

		System.out.println(list.get(0).toSql());

		//Map<String, Object> jsonMap = list.get(0).jsonMaps().get(0);

		//assertThat(jsonMap.get("account_id"), is(2));
	}
}
