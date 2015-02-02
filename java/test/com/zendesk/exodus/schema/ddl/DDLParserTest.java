package com.zendesk.exodus.schema.ddl;

import static org.junit.Assert.*;

import java.io.IOException;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.zendesk.exodus.schema.ddl.ColumnPosition.Position;

import static org.hamcrest.CoreMatchers.*;

public class DDLParserTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	private TableAlter parseAlter(String testAlter) {
		ANTLRInputStream input = new ANTLRInputStream(testAlter);
		mysqlLexer lexer = new mysqlLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);

		// create a parser that feeds off the tokens buffer
		mysqlParser parser = new mysqlParser(tokens);

		ExodusMysqlParserListener listener = new ExodusMysqlParserListener("default_db");

		System.out.println("Running parse on " + testAlter);
		ParseTree tree = parser.parse();

		ParseTreeWalker.DEFAULT.walk(listener, tree);

		return(listener.getAlterStatement());
	}

	@Test
	public void testBasic() {
		ExodusSQLSyntaxRrror e = null;
		assertThat(parseAlter("ALTER TABLE `foo` ADD col1 text"), is(not(nullValue())));
		try {
			parseAlter("ALTER TABLE foolkj `foo` lkjlkj");
		} catch ( ExodusSQLSyntaxRrror err ) {
			e = err;
		}
		assertThat(e, is(not(nullValue())));
	}

	@Test
	public void testColumnAdd() {
		TableAlter a = parseAlter("ALTER TABLE `foo`.`bar` ADD column `col1` text AFTER `afterCol`");
		assertThat(a, is(not(nullValue())));

		AddColumnMod m = (AddColumnMod) a.columnMods.get(0);
		assertThat(m.name, is("col1"));

		assertThat(m.definition, not(nullValue()));

		assertThat(m.position.position, is(ColumnPosition.Position.AFTER));
		assertThat(m.position.afterColumn, is("afterCol"));
	}
}
