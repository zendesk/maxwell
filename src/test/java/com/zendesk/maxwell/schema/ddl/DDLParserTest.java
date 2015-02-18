package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.zendesk.maxwell.schema.columndef.BigIntColumnDef;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

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

	private List<SchemaChange> parse(String sql) {
		return SchemaChange.parse("default_db", sql);
	}

	private TableAlter parseAlter(String sql) {
		return (TableAlter) parse(sql).get(0);
	}

	private TableCreate parseCreate(String sql) {
		return (TableCreate) parse(sql).get(0);
	}

	@Test
	public void testBasic() {
		MaxwellSQLSyntaxError e = null;
		assertThat(parseAlter("ALTER TABLE `foo` ADD col1 text"), is(not(nullValue())));
		try {
			parseAlter("ALTER TABLE foolkj `foo` lkjlkj");
		} catch ( MaxwellSQLSyntaxError err ) {
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
	public void testIntColumnTypes_1() {
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
	public void testIntColumnTypes_2() {
		TableAlter a = parseAlter("alter table `fie` add column baz bigINT null");

		AddColumnMod m = (AddColumnMod) a.columnMods.get(0);
		assertThat(m.name, is("baz"));
		assertThat(m.definition.getTableName(), is("fie"));

		BigIntColumnDef b = (BigIntColumnDef) m.definition;
		assertThat(b.getType(), is("bigint"));
		assertThat(b.getSigned(), is(true));
		assertThat(b.getName(), is("baz"));
	}

	@Test
	public void testVarchar() {
		TableAlter a = parseAlter("alter table no.no add column mocha varchar(255) character set latin1 not null");

		AddColumnMod m = (AddColumnMod) a.columnMods.get(0);
		assertThat(m.name, is("mocha"));
		assertThat(m.definition.getTableName(), is("no"));

		StringColumnDef b = (StringColumnDef) m.definition;
		assertThat(b.getType(), is("varchar"));
		assertThat(b.getEncoding(), is("latin1"));
	}

	@Test
	public void testText() {
		TableAlter a = parseAlter("alter table no.no add column mocha TEXT character set 'utf8' collate 'utf8_foo'");

		AddColumnMod m = (AddColumnMod) a.columnMods.get(0);
		StringColumnDef b = (StringColumnDef) m.definition;
		assertThat(b.getType(), is("text"));
		assertThat(b.getEncoding(), is("utf8"));
	}

	@Test
	public void testDefault() {
		TableAlter a = parseAlter("alter table no.no add column mocha TEXT default 'hello'''''");

		AddColumnMod m = (AddColumnMod) a.columnMods.get(0);
		StringColumnDef b = (StringColumnDef) m.definition;
		assertThat(b.getType(), is("text"));
	}

	@Test
	public void testLots() {
		TableAlter a = parseAlter("alter table bar add column m TEXT character set utf8 "
				+ "default null "
				+ "auto_increment "
				+ "unique key "
				+ "primary key "
				+ "comment 'bar' "
				+ "column_format fixed "
				+ "storage disk");

		AddColumnMod m = (AddColumnMod) a.columnMods.get(0);
		StringColumnDef b = (StringColumnDef) m.definition;
		assertThat(b.getType(), is("text"));
		assertThat(b.getEncoding(), is("utf8"));
	}

	@Test
	public void testMultipleColumns() {
		TableAlter a = parseAlter("alter table bar add column m int(11) unsigned not null, add p varchar(255)");
		assertThat(a.columnMods.size(), is(2));
		assertThat(a.columnMods.get(0).name, is("m"));
		assertThat(a.columnMods.get(1).name, is("p"));
	}

	@Test
	public void testMultipleColumnWithParens() {
		TableAlter a = parseAlter("alter table bar add column (m int(11) unsigned not null, p varchar(255))");
		assertThat(a.columnMods.size(), is(2));
		assertThat(a.columnMods.get(0).name, is("m"));
		assertThat(a.columnMods.get(1).name, is("p"));
	}

	@Test
	public void testParsingSomeAlters() {
		String testSQL[] = {
	       "alter table t add index `foo` using btree (`a`, `cd`) key_block_size=123",
	       "alter table t add key bar (d)",
	       "alter table t add constraint `foo` primary key using btree (id)",
	       "alter table t add primary key (`id`)",
	       "alter table t add constraint unique key (`id`)",
	       "alter table t add fulltext key (`id`)",
	       "alter table t add spatial key (`id`)",
	       "alter table t alter column `foo` SET DEFAULT 112312",
	       "alter table t alter column `foo` SET DEFAULT 1.2",
	       "alter table t alter column `foo` SET DEFAULT 'foo'",
	       "alter table t alter column `foo` drop default",
	       "alter table t CHARACTER SET latin1 COLLATE = 'utf8'",
	       "alter table t DROP PRIMARY KEY",
	       "alter table t drop index `foo`",
	       "alter table t disable keys",
	       "alter table t enable keys",
	       "alter table t order by `foor`, bar"
		};

		for ( String s : testSQL ) {
			TableAlter a = parseAlter(s);
			assertThat("Expected " + s + "to parse", a, not(nullValue()));
		}

	}

	@Test
	public void testChangeColumn() {
		TableAlter a = parseAlter("alter table c CHANGE column `foo` bar int(20) unsigned default 'foo' not null");

		assertThat(a.columnMods.size(), is(1));
		assertThat(a.columnMods.get(0), instanceOf(ChangeColumnMod.class));

		ChangeColumnMod c = (ChangeColumnMod) a.columnMods.get(0);
		assertThat(c.name, is("foo"));
		assertThat(c.definition.getName(), is("bar"));
		assertThat(c.definition.getType(), is("int"));
	}

	@Test
	public void testModifyColumn() {
		TableAlter a = parseAlter("alter table c MODIFY column `foo` int(20) unsigned default 'foo' not null");

		assertThat(a.columnMods.size(), is(1));
		assertThat(a.columnMods.get(0), instanceOf(ChangeColumnMod.class));

		ChangeColumnMod c = (ChangeColumnMod) a.columnMods.get(0);
		assertThat(c.name, is("foo"));
		assertThat(c.definition.getName(), is("foo"));

		assertThat(c.definition.getType(), is("int"));
	}


	@Test
	public void testDropColumn() {
		RemoveColumnMod remove;
		TableAlter a = parseAlter("alter table c drop column `drop`");

		assertThat(a.columnMods.size(), is(1));

		assertThat(a.columnMods.get(0), instanceOf(RemoveColumnMod.class));

		remove = (RemoveColumnMod) a.columnMods.get(0);

		assertThat(remove.name, is("drop"));
	}

	@Test
	public void testRenameTable() {
		TableAlter a = parseAlter("alter table c rename to `foo`");

		assertThat(a.newTableName, is("foo"));

		a = parseAlter("alter table c rename to `foo`.`bar`");
		assertThat(a.newDatabase, is("foo"));
		assertThat(a.newTableName, is("bar"));
	}


	@Test
	public void testConvertCharset() {
		TableAlter a = parseAlter("alter table c convert to character set 'latin1'");
		assertThat(a.convertCharset, is("latin1"));

		a = parseAlter("alter table c charset=utf8");
		assertThat(a.defaultCharset, is("utf8"));

		a = parseAlter("alter table c character set = 'utf8'");
		assertThat(a.defaultCharset, is("utf8"));
	}

	@Test
	public void testCreateTable() {
		TableCreate c = parseCreate("CREATE TABLE `foo` ( id int(11) auto_increment not null, `textcol` mediumtext character set 'utf8' not null )");

		assertThat(c.dbName,  is("default_db"));
		assertThat(c.tableName, is("foo"));

		assertThat(c.columns.size(), is(2));
		assertThat(c.columns.get(0).getName(), is("id"));
		assertThat(c.columns.get(1).getName(), is("textcol"));
	}

	@Test
	public void testCreateTableWithIndexes() {
		TableCreate c = parseCreate(
				"CREATE TABLE `bar`.`foo` ("
			  	  + "id int(11) auto_increment PRIMARY KEY, "
				  + "dt datetime, "
				  + "KEY `index_on_datetime` (dt), "
				  + "KEY (`something else`), "
				  + "INDEX USING BTREE (yet_again)"
				+ ")");
		assertThat(c, not(nullValue()));
	}

	@Test
	public void testCreateTableWithOptions() {
		TableCreate c = parseCreate(
				"CREATE TABLE `bar`.`foo` ("
			  	  + "id int(11) auto_increment PRIMARY KEY"
				+ ") "
			  	+ "ENGINE=innodb "
				+ "CHARACTER SET='latin1' "
			  	+ "ROW_FORMAT=FIXED"
		);
		assertThat(c, not(nullValue()));
	}

	@Test
	public void testDropTable() {
		List<SchemaChange> changes = parse("DROP TABLE IF exists `foo`.bar, `bar`.baz");
		assertThat(changes.size(), is(2));

		TableDrop d = (TableDrop) changes.get(0);
		assertThat(d.tableName, is("bar"));
		assertThat(d.dbName, is("foo"));

	}

	@Test
	public void testCreateDatabase() {
		List<SchemaChange> changes = parse("CREATE DATABASE if not exists `foo` default character set='latin1'");
		DatabaseCreate create = (DatabaseCreate) changes.get(0);
		assertThat(create.dbName, is("foo"));
		assertThat(create.encoding, is("latin1"));
	}
}
