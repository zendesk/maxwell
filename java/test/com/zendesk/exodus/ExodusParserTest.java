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
	private SchemaCapturer capturer;
	private Schema schema;
	private ExodusParser parser;

	@Before
	public void setUp() throws Exception {
		this.capturer = new SchemaCapturer(server.getConnection());
	}

	private List<ExodusAbstractRowsEvent>getRowsForSQL(String queries[]) throws Exception {
		BinlogPosition start = BinlogPosition.capture(server.getConnection());
		ExodusParser p = new ExodusParser(server.getBaseDir() + "/mysqld", start.getFile());
		ArrayList<ExodusAbstractRowsEvent> list = new ArrayList<>();

		p.setSchema(capturer.capture());

        server.executeList(Arrays.asList(queries));

        p.setStartOffset(start.getOffset());

        ExodusAbstractRowsEvent e;
        while ( (e = p.getEvent()) != null )
        	list.add(e);

        return list;

	}

	@Override
	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void testGetEvent() throws Exception {
		List<ExodusAbstractRowsEvent> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		list = getRowsForSQL(input);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).toSql(), is("REPLACE INTO `minimal` (`id`, `account_id`, `text_field`) VALUES (1,1,'hello')"));
	}

}
