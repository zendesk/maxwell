package com.zendesk.exodus.schema.ddl;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.zendesk.exodus.schema.columndef.BigIntColumnDef;
import com.zendesk.exodus.schema.columndef.IntColumnDef;

import ch.qos.logback.core.subst.Token;

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

		for ( org.antlr.v4.runtime.Token t : tokens.getTokens()) {
			System.out.println(t.toString());
		}
		// create a parser that feeds off the tokens buffer
		mysqlParser parser = new mysqlParser(tokens);

		ExodusMysqlParserListener listener = new ExodusMysqlParserListener("default_db");

		System.out.println("Running parse on " + testAlter);
		ParseTree tree = parser.parse();

		ParseTreeWalker.DEFAULT.walk(listener, tree);
		System.out.println(tree.toStringTree(parser));

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

	@Test
	public void testColumnTypes_1() {
		TableAlter a = parseAlter("alter table foo add column `int` int(11) unsigned not null AFTER `afterCol`");

		AddColumnMod m = (AddColumnMod) a.columnMods.get(0);
		assertThat(m.name, is("int"));

		assertThat(m.definition, instanceOf(IntColumnDef.class));
		IntColumnDef i = (IntColumnDef) m.definition;
		assertThat(i.getName(), is("int"));
		assertThat(i.getType(), is("int"));
		assertThat(i.getSigned(), is(false));
	}

	@Test
	public void testColumnTypes_2() {
		TableAlter a = parseAlter("alter table `fie` add column baz bigINT null");

		AddColumnMod m = (AddColumnMod) a.columnMods.get(0);
		assertThat(m.name, is("baz"));
		assertThat(m.definition.getTableName(), is("fie"));

		BigIntColumnDef b = (BigIntColumnDef) m.definition;
		assertThat(b.getType(), is("bigint"));
		assertThat(b.getSigned(), is(true));
		assertThat(b.getName(), is("baz"));
	}
}
