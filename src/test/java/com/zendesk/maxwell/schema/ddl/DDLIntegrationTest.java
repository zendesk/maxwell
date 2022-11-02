package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.*;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.MysqlVersion;
import com.zendesk.maxwell.row.RowMap;
import org.junit.Test;
import static org.junit.Assume.assumeFalse;

import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class DDLIntegrationTest extends MaxwellTestWithIsolatedServer {
	private MaxwellOutputConfig ddlOutputConfig() {
		MaxwellOutputConfig config = new MaxwellOutputConfig();
		config.outputDDL = true;
		return config;
	}

	private void testIntegration(String[] alters) throws Exception {
		MaxwellTestSupport.testDDLFollowing(server, alters);
	}

	private void testIntegration(String sql) throws Exception {
		String[] alters = {sql};
		testIntegration(alters);
	}

	@Test
	public void testAlter() throws Exception {
		String sql[] = {
			"create table shard_1.testAlter ( id int(11) unsigned default 1, str varchar(255) )",
			"alter table shard_1.testAlter add column barbar tinyint",
			"alter table shard_1.testAlter add column thiswillbeutf16 text, engine=`innodb` CHARACTER SET utf16",
			"alter table shard_1.testAlter rename to shard_1.`freedonia`",
			"rename table shard_1.`freedonia` to shard_1.ducksoup, shard_1.ducksoup to shard_1.`nananana`",
			"alter table shard_1.nananana drop column barbar",

			"create table shard_2.weird_rename ( str mediumtext )",
			"alter table shard_2.weird_rename rename to lowball", // renames to shard_1.lowball

			"create table shard_1.testDrop ( id int(11) )",
			"drop table shard_1.testDrop",
			"create table test.c ( v varchar(255) charset ascii )",
		};
		testIntegration(sql);
	}

	@Test
	public void testAlterDatabase() throws Exception {
		String sql[] = {
			"create DATABASE test_db default character set='utf8'",
			"alter schema test_db collate = 'binary'",
			"alter schema test_db character set = 'latin2'"
		};

		testIntegration(sql);
	}

	@Test
	public void testMultiLineSQLWithBlacklists() throws Exception {
		String sql[] = {
			"create table foo (\nbegin_field int)",
		};

		testIntegration(sql);
	}

	@Test
	public void testAlterMultipleColumns() throws Exception {
		String sql[] = {
			"create table shard_1.test_foo ( id int )",
			"alter table shard_1.test_foo add column ( a varchar(255), b varchar(255), primary key (a) )"
		};

		testIntegration(sql);
	}
	@Test
	public void testDrop() throws Exception {
		String sql[] = {
			"create table shard_1.testAlter ( id int(11) unsigned default 1, str varchar(255) )",
			"drop table if exists lasdkjflaskd.laskdjflaskdj",
			"drop table shard_1.testAlter"
		};

		testIntegration(sql);
	}

	@Test
	public void testJSON() throws Exception {
		requireMinimumVersion(server.VERSION_5_7);
		assumeFalse(MysqlIsolatedServer.getVersion().isMariaDB);

		String sql[] = {
			"create table shard_1.testJSON ( j json )",
		};

		testIntegration(sql);
	}

	@Test
	public void testCreateAndDropDatabase() throws Exception {
		String sql[] = {
			"create DATABASE test_db default character set='utf8'",
			"create DATABASE test_db_1 default char set='utf8'",
			"create DATABASE if not exists test_db",
			"create DATABASE test_db_2",
			"drop DATABASE test_db"
		};

		testIntegration(sql);
	}

	@Test
	public void testCreateTableLike() throws Exception {
		String sql[] = {
			"create TABLE `source_tbl` ( str varchar(255) character set latin1, redrum bigint(20) unsigned ) default charset 'latin1'",
			"create TABLE `dest_tbl` like `source_tbl`",
			"create database test_like default charset 'utf8'",
			"create table `test_like`.`foo` LIKE `shard_1`.`source_tbl`"
		};

		testIntegration(sql);
	}


	@Test
	public void testCreateIfNotExists() throws Exception {
		String sql[] = {
				"create TABLE IF NOT EXISTS `duplicateTable` (id int(11) unsigned primary KEY)",
				"create TABLE IF NOT EXISTS `duplicateTable` ( str varchar(255) )",
		};

		testIntegration(sql);
	}

	@Test
	public void testConstraintCheck() throws Exception {
		String sql[] = {
				"create TABLE `t` (id int, CHECK(LENGTH(c) is not null and 'lfjd()))()' is not null), c varchar(255))",
				"create TABLE `t2` (id int, CHECK(LENGTH(c) is not null), c varchar(255))",
				"create table t1 (a int, b int, check (a>b))"
		};

		testIntegration(sql);
	}
	@Test
	public void testDatabaseCharset() throws Exception {
		String sql[] = {
			"create DATABASE test_latin1 character set='latin1'",
			"create TABLE `test_latin1`.`latin1_table` ( id int(11) unsigned, str varchar(255) )",
			"create TABLE `test_latin1`.`utf8_table` ( id int(11) unsigned, "
				+ "str_utf8 varchar(255), "
				+ "str_latin1 varchar(255) character set latin1) charset 'utf8'",
			"alter DATABASE test_latin1 character set='latin2'"
		};

		testIntegration(sql);
	}

	@Test
	public void testConvertCharset() throws Exception {
		String sql[] = {
			"CREATE TABLE t ( a varchar(255) character set latin1, b varchar(255) character set latin2, c blob, d varbinary(255), e varchar(255) binary)",
			"ALTER TABLE t convert to character set 'utf8'"
		};
		testIntegration(sql);
	}

	@Test
	public void testModifyAndMoveColumn() throws Exception {
		String sql[] = {
			"CREATE TABLE t ( a varchar(255), b int)",
			"ALTER TABLE t modify column a varchar(255) after b"
		};
		testIntegration(sql);

	}

	@Test
	public void testAddQualifiedColumn() throws Exception {
		MaxwellTestSupport.assertMaximumVersion(server, new MysqlVersion(8, 0));

		String sql[] = {
			"create TABLE `foo` (id int(11) unsigned primary KEY)",
			"alter table foo add column foo.a varchar(255)",
			"alter table foo add column shard_1.foo.b varchar(255)",
			"alter table foo drop column shard_1.foo.b"
		};

		testIntegration(sql);
	}


	@Test
	public void testPKs() throws Exception {
		String sql[] = {
		   "create TABLE `test_pks` ( id int(11) unsigned primary KEY, str varchar(255) )",
		   "create TABLE `test_pks_2` ( id int(11) unsigned, str varchar(255), primary key(id, str) )",
		   "create TABLE `test_pks_3` ( id int(11) unsigned primary KEY, str varchar(255) )",
		   "create TABLE `test_pks_4` ( id int(11) unsigned primary KEY, str varchar(255) )",
		   "alter TABLE `test_pks_3` drop primary key, add primary key(str)",
		   "alter TABLE `test_pks_4` drop primary key",
		   "alter TABLE `test_pks` change id renamed_id int(11) unsigned",
		   "alter TABLE `test_pks` drop column renamed_id"
		};

		testIntegration(sql);
	}

	@Test
	public void testIntX() throws Exception {
		String sql[] = {
			"create TABLE `test_int1` ( id int1 )",
			"create TABLE `test_int2` ( id INT2 )",
			"create TABLE `test_int3` ( id int3 )",
			"create TABLE `test_int4` ( id int4 )",
			"create TABLE `test_int8` ( id int8 )"
		};

		testIntegration(sql);
	}

	@Test
	public void testSerial() throws Exception {
		String sql[] = {
			"create TABLE `test_int1` ( id serial )"
		};

		testIntegration(sql);
	}

	@Test
	public void testYearWithLength() throws Exception {
		String sql[] = {
			"create TABLE `test_year` ( id year(4) )"
		};

		testIntegration(sql);
	}

	@Test
	public void testTimeWithLength() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);

		String sql[] = {
			"create TABLE `test_time` ( id time(3) )"
		};

		testIntegration(sql);
	}

	@Test
	public void testDatetimeWithLength() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);

		String sql[] = {
			"create TABLE `test_datetime` ( id datetime(3) )",
			"alter TABLE `test_datetime` add column ts timestamp(6)"
		};

		testIntegration(sql);
	}

	@Test
	public void testTimestampWithLength() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);

		String sql[] = {
			"create TABLE `test_year` ( id timestamp(3) )"
		};

		testIntegration(sql);
	}

	@Test
	public void testBooleans() throws Exception {
		String sql[] = {
			"create TABLE `test_boolean` ( b1 bool, b2 boolean )"
		};

		testIntegration(sql);
	}

	@Test
	public void testReals() throws Exception {
		String sql[] = {
			"create TABLE `test_reals` ( r1 REAL, b2 REAL (2,2) )"
		};

		testIntegration(sql);
	}

	@Test
	public void testNumericNames() throws Exception {
		String sql[] = {
			"create TABLE shard_1.20151214_foo ( r1 REAL, b2 REAL (2,2) )",
			"create TABLE shard_1.20151214 ( r1 REAL, b2 REAL (2,2) )"
		};

		testIntegration(sql);
	}

	@Test
	public void testLongStringColumns() throws Exception {
		String sql[] = {
			"create TABLE t1( a long varchar character set 'utf8' )",
			"create TABLE t2( a long varbinary )",
			"create TABLE t3( a long binary character set 'latin1' default NULL )",
			"create table t4( a long )"
		};

		testIntegration(sql);
	}

	@Test
	public void testCompatibilityColumnTypes() throws Exception {
		String sql[] = {
			"create TABLE t1( a FIXED )",
			"create TABLE t2( a float4 )",
			"create TABLE t3( a float8 )",
			"create table t4( a middleint )",
			"create table t5( a numeric )",

		};

		testIntegration(sql);
	}

	@Test
	public void testASCIICharset() throws Exception {
		String sql[] = {
			"create TABLE t1( a varchar(255) ASCII, b enum('a', 'b') ASCII )"
		};

		testIntegration(sql);
	}

	@Test
	public void testDoubleQuotedTables() throws Exception {
		String sql[] = {
			"create DATABASE \"tt_db\"",
			"create table \"tt_db\".\"tt_tt\" ( \"id\" int )",
			"create table \"tt_db\".\"`weird_quote`\" ( \"id\" int )",
		};

		server.execute("SET @old_mode = @@SESSION.sql_mode");
		server.execute("SET SESSION sql_mode = CONCAT('ANSI_QUOTES,', @@SESSION.sql_mode)");


		testIntegration(sql);
		server.execute("SET SESSION sql_mode = @old_mode");
	}


	@Test
	public void testNationChar() throws Exception {
		testIntegration("create table t1 ( a CHAR(10) CHARACTER SET utf8, " +
			"b NATIONAL CHARACTER(10), " +
			"c NCHAR(10), " +
			"d VARCHAR(10) CHARACTER SET utf8, " +
			"e NATIONAL VARCHAR(10), " +
			"f NVARCHAR(10), " +
			"g NCHAR VARCHAR(10), " +
			"h NATIONAL CHARACTER VARYING(10), " +
			"i NATIONAL CHAR VARYING(10), " +
			"j CHARACTER, " +
			"k CHARACTER VARYING(10)" +
			") default character set=latin1"
		);
	}

	@Test
	public void testUnicodeKeywork() throws Exception {
		testIntegration("create table t1 ( a CHAR(10) UNICODE, " +
				"d VARCHAR(10) UNICODE, " +
				"h CHARACTER VARYING(10) UNICODE, " +
				"j CHARACTER UNICODE, " +
				"k TEXT(20) UNICODE " +
				") default character set=latin1"
		);
	}
	@Test
	public void testAutosizingColumns() throws Exception {
		testIntegration("create table t1 ( " +
			"a text(1), " +
			"b text(256), " +
			"c text(65536), " +
			"d text(16777216), " +
			"e blob(1), " +
			"f blob(256), " +
			"g blob(65536), " +
			"h blob(16777216), " +
			"i text, " +
			"j blob)"
		);
	}

	@Test
	public void testCaseSensitiveDatabases() throws Exception {
		if ( buildContext().getCaseSensitivity() == CaseSensitivity.CASE_SENSITIVE ) {
			String sql[] = {
				"create TABLE taaaayble( a long varchar character set 'utf8' )",
				"create TABLE TAAAAYBLE( a long varbinary )",
				"drop table taaaayble"
			};

			testIntegration(sql);
		}
	}

	@Test
	public void testAutoConvertToByte() throws Exception {
		testIntegration("create table t1 ( " +
			"a char(1) byte, " +
			"b varchar(255) byte, " +
			"c tinytext byte, " +
			"d text byte, " +
			"e mediumtext byte, " +
			"f longtext byte, " +
			"g character varying(255) byte, " +
			"h long byte, " +
			"i text(234344) byte" +
			")"
		);
	}

	@Test
	@Category(Mysql57Tests.class)
	public void testGeneratedColumns() throws Exception {
		requireMinimumVersion(server.VERSION_5_7, false);
		testIntegration("create table t ("
			+ "a INT GENERATED ALWAYS AS (0) VIRTUAL UNIQUE NOT NULL, "
			+ "b int AS (a + 0) STORED PRIMARY KEY"
			+ ")"
		);
	}

	@Test
	public void testRenameColumn() throws Exception {
		requireMinimumVersion(8,0);
		String sql[] = {
			"CREATE TABLE foo ( i int )",
			"ALTER TABLE foo rename column i to j"
		};
		testIntegration(sql);
	}

	@Test
	public void testTableCreate() throws Exception {
		String[] sql = {"create table TestTableCreate1 ( account_id int, text_field text )"};
		List<RowMap> rows = getRowsForDDLTransaction(sql, null);
		assertEquals(1, rows.size());
		assertTrue(rows.get(0).toJSON(ddlOutputConfig()).contains("\"type\":\"table-create\",\"database\":\"mysql\",\"table\":\"TestTableCreate1\""));
	}
	
	@Test
	public void testNonLatinTableCreate() throws Exception {
		String[] sql = {"create table 測試表格 ( 測試欄位一 int, 測試欄位二 text )"};
		List<RowMap> rows = getRowsForDDLTransaction(sql, null);
		assertEquals(1, rows.size());
		assertTrue(rows.get(0).toJSON(ddlOutputConfig()).contains("\"type\":\"table-create\",\"database\":\"mysql\",\"table\":\"測試表格\""));
	}

	@Test
	public void testTableCreateFilter() throws Exception {
		String[] sql = {"create table TestTableCreate2 ( account_id int, text_field text )"};
		List<RowMap> rows = getRowsForDDLTransaction(sql, excludeTable("TestTableCreate2"));
		assertEquals(0, rows.size());
	}
	
	@Test
	public void testNonLatinTableCreateFilter() throws Exception {
		String[] sql = {"create table 測試表格二 ( 測試欄位一 int, 測試欄位二 text )"};
		List<RowMap> rows = getRowsForDDLTransaction(sql, excludeTable("測試表格二"));
		assertEquals(0, rows.size());
	}

	@Test
	public void testTableRenameFilter() throws Exception {
		String[] sql = {
			"create table TestTableCreate3 ( account_id int, text_field text )",
			"rename table TestTableCreate3 to TestTableCreate4"
		};
		List<RowMap> rows = getRowsForDDLTransaction(sql, excludeTable("TestTableCreate4"));
		assertEquals(1, rows.size());
		assertTrue(rows.get(0).toJSON(ddlOutputConfig()).contains("\"type\":\"table-create\",\"database\":\"mysql\",\"table\":\"TestTableCreate3\""));
	}
	
	@Test
	public void testNonLatinTableRenameFilter() throws Exception {
		String[] sql = {
			"create table 測試表格三 ( 測試欄位一 int, 測試欄位二 text )",
			"rename table 測試表格三 to 測試表格四"
		};
		List<RowMap> rows = getRowsForDDLTransaction(sql, excludeTable("測試表格四"));
		assertEquals(1, rows.size());
		assertTrue(rows.get(0).toJSON(ddlOutputConfig()).contains("\"type\":\"table-create\",\"database\":\"mysql\",\"table\":\"測試表格三\""));
	}
	

	@Test
	public void testDatabaseCreate() throws Exception {
		assumeFalse(MysqlIsolatedServer.getVersion().getMajor() == 8);
		String[] sql = {
			"create database TestDatabaseCreate1",
			"alter database TestDatabaseCreate1 character set latin2"
		};
		List<RowMap> rows = getRowsForDDLTransaction(sql, null);
		assertEquals(2, rows.size());
		assertTrue(rows.get(0).toJSON(ddlOutputConfig()).contains("\"type\":\"database-create\",\"database\":\"TestDatabaseCreate1\""));
		assertTrue(rows.get(1).toJSON(ddlOutputConfig()).contains("\"type\":\"database-alter\",\"database\":\"TestDatabaseCreate1\""));
	}
	
	@Test
	public void testNonLatinDatabaseCreate() throws Exception {
		assumeFalse(MysqlIsolatedServer.getVersion().getMajor() == 8);
		String[] sql = {
			"create database 測試資料庫一",
			"alter database 測試資料庫一 character set latin2"
		};
		List<RowMap> rows = getRowsForDDLTransaction(sql, null);
		assertEquals(2, rows.size());
		assertTrue(rows.get(0).toJSON(ddlOutputConfig()).contains("\"type\":\"database-create\",\"database\":\"測試資料庫一\""));
		assertTrue(rows.get(1).toJSON(ddlOutputConfig()).contains("\"type\":\"database-alter\",\"database\":\"測試資料庫一\""));
	}

	@Test
	public void testDatabaseFilter() throws Exception {
		String[] sql = {"create database TestDatabaseCreate2"};
		List<RowMap> rows = getRowsForDDLTransaction(sql, excludeDb("TestDatabaseCreate2"));
		assertEquals(0, rows.size());
	}

	@Test
	public void testNonLatinDatabaseFilter() throws Exception {
		String[] sql = {"create database 測試資料庫二"};
		List<RowMap> rows = getRowsForDDLTransaction(sql, excludeDb("測試資料庫二"));
		assertEquals(0, rows.size());
	}

	@Test
	public void testDatabaseChangeWithTableFilter() throws Exception {
		String[] sql = {
				"create database TestDatabaseCreate3",
				"create table `TestDatabaseCreate3`.`chicken` ( id int )",
				"create table `TestDatabaseCreate3`.`burger` ( id int )"
		};
		List<RowMap> rows = getRowsForDDLTransaction(sql, excludeTable("chicken"));
		assertEquals(2, rows.size());
		assertTrue(rows.get(0).toJSON(ddlOutputConfig()).contains("\"type\":\"database-create\",\"database\":\"TestDatabaseCreate3\""));
		assertTrue(rows.get(1).toJSON(ddlOutputConfig()).contains("\"type\":\"table-create\",\"database\":\"TestDatabaseCreate3\",\"table\":\"burger\""));
	}
	
	@Test
	public void testNonLatinDatabaseChangeWithTableFilter() throws Exception {
		String[] sql = {
				"create database 測試資料庫三",
				"create table `測試資料庫三`.`表格一` ( 中文欄位 int )",
				"create table `測試資料庫三`.`表格二` ( 中文欄位 int )"
		};
		List<RowMap> rows = getRowsForDDLTransaction(sql, excludeTable("表格一"));
		assertEquals(2, rows.size());
		assertTrue(rows.get(0).toJSON(ddlOutputConfig()).contains("\"type\":\"database-create\",\"database\":\"測試資料庫三\""));
		assertTrue(rows.get(1).toJSON(ddlOutputConfig()).contains("\"type\":\"table-create\",\"database\":\"測試資料庫三\",\"table\":\"表格二\""));
		// test if non-Latin column name outputs correctly
		assertTrue(rows.get(1).toJSON(ddlOutputConfig()).contains("\"type\":\"int\",\"name\":\"中文欄位\""));
	}

	@Test
	public void testHandleReferencingRenamedColumn() throws Exception {
		String [] sql = {
			"create table t ( a int, b varchar(255))",
			"alter table t change column a a int after b_gets_renamed, change column b b_gets_renamed varchar(255)",
			"create table tt ( a int, b smallint )",
			"alter table tt add column c int after a_gets_renamed, change column a a_gets_renamed int"
		};

		testIntegration(sql);
	}

	@Test
	public void testImplicitDatabaseInAlter() throws Exception {
		String [] sql = {
			"create database ohgod",
			"USE ohgod",
			"ALTER DATABASE CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci"
		};
		testIntegration(sql);
	}

	@Test
	public void testAlterIgnoreMaria() throws Exception {
		assumeTrue(MysqlIsolatedServer.getVersion().isMariaDB);

		String [] sql = {
			"create table foo ( colA int, colB int)",
			"ALTER IGNORE TABLE foo ADD CONSTRAINT table_name_pk PRIMARY KEY IF NOT EXISTS (colA, colB)"
		};
		testIntegration(sql);
	}
}
