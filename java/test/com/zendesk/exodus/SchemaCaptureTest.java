package com.zendesk.exodus;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.exodus.schema.Schema;
import com.zendesk.exodus.schema.SchemaCapturer;

public class SchemaCaptureTest extends AbstractMaxwellTest {

	@Before
	public void setUp() throws Exception {
	}

	@Override
	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void test() throws SQLException {
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection());
		Schema s = capturer.capture();


	}

}
