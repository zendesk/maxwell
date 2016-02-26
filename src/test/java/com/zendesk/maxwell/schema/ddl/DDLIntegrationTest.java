package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.zendesk.maxwell.CaseSensitivity;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.maxwell.AbstractMaxwellTest;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;

public class DDLIntegrationTest extends AbstractMaxwellTest {
	@Before
	public void setUp() throws Exception {
	}

	@Override
	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}

	private Schema testIntegration(String alters[]) throws SQLException, SchemaSyncError, IOException {
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection(), buildContext().getCaseSensitivity());
		Schema topSchema = capturer.capture();

		server.executeList(Arrays.asList(alters));

		for ( String alterSQL : alters) {
			List<SchemaChange> changes = SchemaChange.parse("shard_1", alterSQL);
			if ( changes != null ) {
				for ( SchemaChange change : changes ) {
					topSchema = change.apply(topSchema);
				}
			}
		}

		Schema bottomSchema = capturer.capture();

		List<String> diff = topSchema.diff(bottomSchema, "followed schema", "recaptured schema");
		assertThat(StringUtils.join(diff.iterator(), "\n"), diff.size(), is(0));

		return topSchema;
	}

	private Schema testIntegration(String sql) throws Exception {
		String[] alters = {sql};
		return testIntegration(alters);
	}

	@Test
	public void testAlter() throws SQLException, SchemaSyncError, IOException, InterruptedException {
		String sql[] = {
			"create table shard_1.testAlter ( id int(11) unsigned default 1, str varchar(255) )",
			"alter table shard_1.testAlter add column barbar tinyint",
			"alter table shard_1.testAlter rename to shard_1.`freedonia`",
			"rename table shard_1.`freedonia` to shard_1.ducksoup, shard_1.ducksoup to shard_1.`nananana`",
			"alter table shard_1.nananana drop column barbar",

			"create table shard_2.weird_rename ( str mediumtext )",
			"alter table shard_2.weird_rename rename to lowball", // renames to shard_1.lowball

			"create table shard_1.testDrop ( id int(11) )",
			"drop table shard_1.testDrop"

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
	public void testAlterMultipleColumns() throws Exception {
		String sql[] = {
			"create table shard_1.test_foo ( id int )",
			"alter table shard_1.test_foo add column ( a varchar(255), b varchar(255), primary key (a) )"
		};

		testIntegration(sql);
	}
	@Test
	public void testDrop() throws SQLException, SchemaSyncError, IOException, InterruptedException {
		String sql[] = {
			"create table shard_1.testAlter ( id int(11) unsigned default 1, str varchar(255) )",
			"drop table if exists lasdkjflaskd.laskdjflaskdj",
			"drop table shard_1.testAlter"
		};

		testIntegration(sql);
	}

	@Test
	public void testCreateAndDropDatabase() throws Exception {
		String sql[] = {
			"create DATABASE test_db default character set='utf8'",
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
				"create TABLE `t` (id int, CHECK(NOW() is not null and 'lfjd()))()' is not null), c varchar(255))",
				"create TABLE `t2` (id int, CHECK(NOW() is not null), c varchar(255))",
				"create table t1 (a int, b int, check (a>b))"
		};

		testIntegration(sql);
	}
	@Test
	public void testDatabaseCharset() throws SQLException, SchemaSyncError, IOException {
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
	public void testPKs() throws SQLException, SchemaSyncError, IOException {
		String sql[] = {
		   "create TABLE `test_pks` ( id int(11) unsigned primary KEY, str varchar(255) )",
		   "create TABLE `test_pks_2` ( id int(11) unsigned, str varchar(255), primary key(id, str) )",
		   "create TABLE `test_pks_3` ( id int(11) unsigned primary KEY, str varchar(255) )",
		   "create TABLE `test_pks_4` ( id int(11) unsigned primary KEY, str varchar(255) )",
		   "alter TABLE `test_pks_3` drop primary key, add primary key(str)",
		   "alter TABLE `test_pks_4` drop primary key"
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
	public void testYearWithLength() throws Exception {
		String sql[] = {
			"create TABLE `test_year` ( id year(4) )"
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
	public void testASCIICharset() throws Exception {
		String sql[] = {
			"create TABLE t1( a varchar(255) ASCII, b enum('a', 'b') ASCII )"
		};

		testIntegration(sql);
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
	public void testCaseInsensitiveDatabase() throws Exception {
		if (buildContext().getCaseSensitivity() != CaseSensitivity.CASE_SENSITIVE) {
			String sql[] = {
					"create TABLE taybal( a long varchar character set 'utf8' )",
					"alter table TAYbal add column b int",
					"drop table TAYBAL"
			};

			testIntegration(sql);
		}
	}

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
}
