package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import com.zendesk.maxwell.AbstractMaxwellTest;
import org.antlr.v4.runtime.misc.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.zendesk.maxwell.schema.columndef.BigIntColumnDef;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

public class DDLParserTest {
	public String getSQLDir() {
		final String dir = System.getProperty("user.dir");
		return dir + "/src/test/resources/sql/";
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
			"alter database d DEFAULT CHARACTER SET = 'utf8'",
			"alter database d UPGRADE DATA DIRECTORY NAME",
			"alter schema d COLLATE foo",
			"alter table t add index `foo` using btree (`a`, `cd`) key_block_size=123",
			"alter table t add key bar (d)",
			"alter table t add constraint `foo` primary key using btree (id)",
			"alter table t add primary key (`id`)",
			"alter table t add constraint unique key (`id`)",
			"alter table t add fulltext key (`id`)",
			"alter table t add index foo (a desc)",
			"alter table t add index foo (a asc)",
			"alter table t add index foo (a) COMMENT 'hello world'",
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
			"alter table t order by `foor`, bar",
			"alter table tester add index (whatever(20), `f,` (2))",
			"create table t ( id int ) engine = innodb, auto_increment = 5",
			"alter table t engine=innodb",
			"alter table t auto_increment =5",
			"alter table t add column `foo` int, auto_increment = 5 engine=innodb, modify column bar int",
			"alter table t add column `foo` int,  ALGORITHM=copy",
			"alter table t add column `foo` int, algorithm copy",
			"alter table t add column `foo` int, algorithm copy, lock shared",
			"alter table t add column `foo` int, algorithm copy, lock=exclusive",
			"create table t (id int) engine=memory",
			"CREATE TABLE `t1` (id int, UNIQUE `int` (`int`))",
			"create table t2 (b varchar(10) not null unique) engine=MyISAM",
			"create TABLE shard_1.20151214foo ( r1 REAL, b2 REAL (2,2) )",
			"create TABLE shard_1.20151214 ( r1 REAL, b2 REAL (2,2) )",
			"create table `shard1.foo` ( `id.foo` int )"
		};

		for ( String s : testSQL ) {
			SchemaChange parsed = parse(s).get(0);
			assertThat("Expected " + s + "to parse", parsed, not(nullValue()));
		}

	}

	@Test
	public void testSQLBlacklist() {
		String testSQL[] = {
			"ALTER DEFINER=foo VIEW",
			"CREATE VIEW foo",
			"CREATE TRIGGER foo",
			"CREATE DEFINER=`dba`@`localhost` TRIGGER `pt_osc_zd_shard485_prod_cf_values_del` ... ",
			"CREATE EVENT foo ",
			"DROP EVENT foo bar",
			"ALTER ALGORITHM = UNDEFINED DEFINER='view'@'localhost' SQL SECURITY DEFINER VIEW `fooview` as (SELECT * FROM FOO)"
				+ "VIEW view_name [(alskdj lk jdlfka j dlkjd lk",
			"CREATE TEMPORARY TABLE 172898_16841_transmem SELECT t.* FROM map.transmem AS t"
		};

		for ( String s : testSQL ) {
			assertThat(SchemaChange.parse("default_db", s), is(nullValue()));
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
	public void testDecimalWithSingleDigitPrecsion() {
		TableCreate c = parseCreate( "CREATE TABLE test.chk (  group_name DECIMAL(8) NOT NULL)  ");
		assertThat(c, not(nullValue()));
	}

	@Test
	public void testDecimalWithDoubleDigitPrecision() {
		TableCreate c = parseCreate( "CREATE TABLE test.chk (  group_name DECIMAL(8, 2) NOT NULL)  ");
		assertThat(c, not(nullValue()));
	}

	@Test
	public void testNumericType() {
		TableCreate c = parseCreate( "CREATE TABLE test.chk (  group_name NUMERIC(8) NOT NULL)  ");
		assertThat(c, not(nullValue()));
	}

	@Test
	public void testCreateTableLikeTable() {
		TableCreate c = parseCreate("CREATE TABLE `foo` LIKE `bar`.`baz`");

		assertThat(c, not(nullValue()));
		assertThat(c.tableName, is("foo"));

		assertThat(c.likeDB,    is("bar"));
		assertThat(c.likeTable, is("baz"));
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

	@Test
	public void testCreateSchema() {
		List<SchemaChange> changes = parse("CREATE SCHEMA if not exists `foo`");
		DatabaseCreate create = (DatabaseCreate) changes.get(0);
		assertThat(create.dbName, is("foo"));
	}

	@Test
	public void testCommentSyntax() {
		List<SchemaChange> changes = parse("CREATE DATABASE if not exists `foo` default character set='latin1' /* generate by server */");
		assertThat(changes.size(), is(1));
	}

	@Test
	public void testCommentSyntax2() {
		List<SchemaChange> changes = parse("CREATE DATABASE if not exists `foo` -- inline comment!\n default character # another one\nset='latin1' --one at the end");
		assertThat(changes.size(), is(1));
	}

	@Test
	public void testCurrentTimestamp() {
		List<SchemaChange> changes = parse("CREATE TABLE `foo` ( `id` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP )");
		assertThat(changes.size(), is(1));
	}

	@Test
	public void testBinaryChar() {
		List<SchemaChange> changes = parse("CREATE TABLE `foo` ( `id` char(16) BINARY character set 'utf8' )");
		assertThat(changes.size(), is(1));
	}

	@Test
	public void testCharsetPositionIndependence() {
		TableCreate create = parseCreate("CREATE TABLE `foo` (id varchar(1) NOT NULL character set 'foo')");
		assertThat(create.columns.get(0).encoding, is("foo"));

		create = parseCreate("CREATE TABLE `foo` (id varchar(1) character set 'foo' NOT NULL)");
		assertThat(create.columns.get(0).encoding, is("foo"));
	}

	@Test
	public void testCreateTableNamedPrimaryKey() {
		/* not documented, but accepted and ignored to name the primary key. */
		TableCreate create = parseCreate("CREATE TABLE db (foo char(60) binary DEFAULT '' NOT NULL, PRIMARY KEY Host (foo,Db,User))");
		assertThat(create, is(notNullValue()));
		assertThat(create.pks.size(), is(3));
	}

	@Test
	public void testCommentsThatAreNotComments() {
		TableCreate create = parseCreate("CREATE TABLE /*! IF NOT EXISTS */ foo (id int primary key)");
		assertThat(create, is(notNullValue()));
		assertThat(create.ifNotExists, is(true));
	}

	@Test
	public void testBinaryColumnDefaults() {
		assertThat(parseCreate("CREATE TABLE foo (id boolean default true)"), is(notNullValue()));
		assertThat(parseCreate("CREATE TABLE foo (id boolean default false)"), is(notNullValue()));
	}

	@Test
	public void testMysqlTestSQL() throws Exception {
		List<String> lines = Files.readAllLines(Paths.get(getSQLDir() + "/ddl/mysql-test.sql"), Charset.defaultCharset());
		for ( String sql: lines ) {
			parse(sql);
		}
	}
}
