package com.zendesk.maxwell.util;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SqlTest {
	@Test
	public void testQuotesPlainIdentifier() {
		assertThat(Sql.quoteIdentifier("account_id"), is("`account_id`"));
	}

	@Test
	public void testQuotesReservedWord() {
		assertThat(Sql.quoteIdentifier("key"), is("`key`"));
		assertThat(Sql.quoteIdentifier("order"), is("`order`"));
	}

	@Test
	public void testEscapesEmbeddedBacktickByDoubling() {
		assertThat(Sql.quoteIdentifier("fo`o"), is("`fo``o`"));
	}

	@Test
	public void testEscapesMultipleEmbeddedBackticks() {
		assertThat(Sql.quoteIdentifier("`a`b`"), is("```a``b```"));
	}

	@Test
	public void testQuotesEmptyIdentifier() {
		assertThat(Sql.quoteIdentifier(""), is("``"));
	}
}
