package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.maxwell.AbstractMaxwellTest;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;

public class DDLResolverTest extends AbstractMaxwellTest {
	@Before
	public void setUp() throws Exception {
	}

	@Override
	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}


	@Test
	public void testDatabaseCreateResolve() throws Exception {
		server.executeQuery("set global character_set_server = 'latin1'");
		server.resetConnection();

		SchemaCapturer capturer = new SchemaCapturer(server.getConnection(), buildContext().getCaseSensitivity());
		Schema topSchema = capturer.capture();

		assertEquals("latin1", topSchema.getCharset());

		List<SchemaChange> changeList = SchemaChange.parse("CREATE DATABASE `foo`");
		assertThat(changeList.size, is(1));

		DatabaseCreate c = (DatabaseCreate)changeList.get(0);
	}

}
