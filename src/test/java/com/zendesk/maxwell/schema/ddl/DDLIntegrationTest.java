package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

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
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection());
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
	public void testDatabaseEncoding() throws SQLException, SchemaSyncError, IOException {
		String sql[] = {
		   "create DATABASE test_latin1 character set='latin1'",
		   "create TABLE `test_latin1`.`latin1_table` ( id int(11) unsigned, str varchar(255) )",
		   "create TABLE `test_latin1`.`utf8_table` ( id int(11) unsigned, "
		     + "str_utf8 varchar(255), "
		     + "str_latin1 varchar(255) character set latin1) charset 'utf8'"
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
			"create TABLE t3( a long binary character set 'latin1' default NULL )"
		};

		testIntegration(sql);
	}

}
