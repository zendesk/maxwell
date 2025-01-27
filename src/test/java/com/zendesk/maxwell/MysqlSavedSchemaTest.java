package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.DateTimeColumnDef;
import com.zendesk.maxwell.schema.columndef.TimeColumnDef;

public class MysqlSavedSchemaTest extends MaxwellTestWithIsolatedServer {
	private Schema schema;
	private Position position;
	private MysqlSavedSchema savedSchema;
	private CaseSensitivity caseSensitivity = CaseSensitivity.CASE_SENSITIVE;

	String ary[] = {
			"delete from `maxwell`.`positions`",
			"delete from `maxwell`.`schemas`",
			"CREATE TABLE shard_1.latin1 (id int(11), str1 varchar(255), str2 varchar(255) character set 'utf8') charset = 'latin1'",
			"CREATE TABLE shard_1.enums (id int(11), enum_col enum('foo', 'bar', 'baz'))",
			"CREATE TABLE shard_1.pks (id int(11), col2 varchar(255), col3 datetime, PRIMARY KEY(col2, col3, id))",
			"CREATE TABLE shard_1.pks_case (id int(11), Col2 varchar(255), COL3 datetime, PRIMARY KEY(col2, col3))",
			"CREATE TABLE shard_1.signed (badcol int(10) unsigned, CaseCol char)"
	};
	ArrayList<String> schemaSQL = new ArrayList(Arrays.asList(ary));

	private MaxwellContext context;

	@Before
	public void setUp() throws Exception {
		if ( server.getVersion().atLeast(server.VERSION_5_6) ) {
			schemaSQL.add("CREATE TABLE shard_1.time_with_length (id int (11), dt2 datetime(3), ts2 timestamp(6), t2 time(6))");
			schemaSQL.add("CREATE TABLE shard_1.without_col_length (badcol datetime(3))");
		}

		server.executeList(schemaSQL);

		this.position = MaxwellTestSupport.capture(server.getConnection());
		this.context = buildContext(position);
		this.schema = new SchemaCapturer(server.getConnection(), context.getCaseSensitivity()).capture();
		this.savedSchema = new MysqlSavedSchema(this.context, this.schema, position);
	}

	@Test
	public void testSave() throws SQLException, IOException, InvalidSchemaError {
		this.savedSchema.save(context.getMaxwellConnection());

		MysqlSavedSchema restoredSchema = MysqlSavedSchema.restore(context, context.getInitialPosition());
		List<String> diff = this.schema.diff(restoredSchema.getSchema(), "captured schema", "restored schema");
		assertThat(StringUtils.join(diff, "\n"), diff.size(), is(0));
	}

	@Test
	public void testRestorePK() throws Exception {
		this.savedSchema.save(context.getMaxwellConnection());

		MysqlSavedSchema restoredSchema = MysqlSavedSchema.restore(context, context.getInitialPosition());
		Table t = restoredSchema.getSchema().findDatabase("shard_1").findTable("pks");

		assertThat(t.getPKList(), is(not(nullValue())));
		assertThat(t.getPKList().size(), is(3));
		assertThat(t.getPKList().get(0), is("col2"));
		assertThat(t.getPKList().get(1), is("col3"));
		assertThat(t.getPKList().get(2), is("id"));
	}

	@Test
	public void testPKCase() throws Exception {
		this.savedSchema.save(context.getMaxwellConnection());

		MysqlSavedSchema restoredSchema = MysqlSavedSchema.restore(context, context.getInitialPosition());
		Table t = restoredSchema.getSchema().findDatabase("shard_1").findTable("pks_case");

		assertThat(t.getPKList().get(0), is("Col2"));
		assertThat(t.getPKList().get(1), is("COL3"));
	}

	@Test
	public void testTimeWithLengthCase() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);

		this.savedSchema.save(context.getMaxwellConnection());

		MysqlSavedSchema restored = MysqlSavedSchema.restore(context, context.getInitialPosition());

		DateTimeColumnDef cd = (DateTimeColumnDef) restored.getSchema().findDatabase("shard_1").findTable("time_with_length").findColumn("dt2");
		assertThat(cd.getColumnLength(), is(3L));

		cd = (DateTimeColumnDef) restored.getSchema().findDatabase("shard_1").findTable("time_with_length").findColumn("ts2");
		assertThat(cd.getColumnLength(), is(6L));

		TimeColumnDef cd2 = (TimeColumnDef) restored.getSchema().findDatabase("shard_1").findTable("time_with_length").findColumn("t2");
		assertThat(cd2.getColumnLength(), is(6L));
	}

	@Test
	public void testFixUnsignedColumnBug() throws Exception {
		Connection c = context.getMaxwellConnection();
		this.savedSchema.save(c);

		c.createStatement().executeUpdate("update maxwell.schemas set version = 0 where id = " + this.savedSchema.getSchemaID());
		c.createStatement().executeUpdate("update maxwell.columns set is_signed = 1 where name = 'badcol'");

		MysqlSavedSchema restored = MysqlSavedSchema.restore(context, context.getInitialPosition());
		IntColumnDef cd = (IntColumnDef) restored.getSchema().findDatabase("shard_1").findTable("signed").findColumn("badcol");
		assertThat(cd.isSigned(), is(false));
	}

	@Test
	public void testFixColumnCasingOnUpgrade() throws Exception {
		Connection c = context.getMaxwellConnection();
		this.savedSchema.save(c);

		c.createStatement().executeUpdate("update maxwell.schemas set version = 1 where id = " + this.savedSchema.getSchemaID());
		c.createStatement().executeUpdate("update maxwell.columns set name = 'casecol' where name = 'CaseCol'");

		MysqlSavedSchema restored = MysqlSavedSchema.restore(context, context.getInitialPosition());
		ColumnDef cd = restored.getSchema().findDatabase("shard_1").findTable("signed").findColumn("casecol");
		assertThat(cd.getName(), is("CaseCol"));
	}

	@Test
	public void testUpgradeSchemaStore() throws Exception {
		Connection c = context.getMaxwellConnection();
		c.createStatement().executeUpdate("alter table `maxwell`.`schemas` drop column deleted, " +
				"drop column base_schema_id, drop column deltas, drop column version, drop column position_sha");
		c.createStatement().executeUpdate("alter table maxwell.positions drop primary key, drop column client_id, add primary key (server_id)");
		c.createStatement().executeUpdate("alter table maxwell.positions drop column gtid_set");
		c.createStatement().executeUpdate("alter table maxwell.schemas drop column gtid_set");
		c.createStatement().executeUpdate("alter table `columns` " +
				"modify id int unsigned NOT NULL AUTO_INCREMENT, " +
				"modify schema_id int unsigned, " +
				"modify table_id int unsigned");
		c.createStatement().executeUpdate("alter table `tables` " +
				"modify id int unsigned NOT NULL AUTO_INCREMENT, " +
				"modify schema_id int unsigned, " +
				"modify database_id int unsigned");
		c.createStatement().executeUpdate("alter table `databases` " +
				"modify id int unsigned NOT NULL AUTO_INCREMENT, " +
				"modify schema_id int unsigned");
		c.createStatement().executeUpdate("alter table `schemas` modify id int unsigned NOT NULL AUTO_INCREMENT");
		c.createStatement().executeUpdate("alter table `bootstrap` modify id int unsigned NOT NULL AUTO_INCREMENT");
		SchemaStoreSchema.upgradeSchemaStoreSchema(c); // just verify no-crash.
	}

	@Test
	public void testUpgradeAddColumnLength() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);

		Connection c = context.getMaxwellConnection();
		this.savedSchema.save(c);
		c.createStatement().executeUpdate("alter table `maxwell`.`columns` drop column column_length");
		SchemaStoreSchema.upgradeSchemaStoreSchema(c); // just verify no-crash.

		Schema schemaBefore = MysqlSavedSchema.restoreFromSchemaID(this.savedSchema.getSchemaID(), c, context.getCaseSensitivity()).getSchema();
		DateTimeColumnDef cd1 = (DateTimeColumnDef) schemaBefore.findDatabase("shard_1").findTable("without_col_length").findColumn("badcol");

		assertEquals((Long) 0L, (Long) cd1.getColumnLength());
	}

	@Test
	public void testUpgradeAddColumnLengthForExistingSchemas() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);

		Connection c = context.getMaxwellConnection();
		this.savedSchema.save(c);
		c.createStatement().executeUpdate("update maxwell.schemas set version = 2 where id = " + this.savedSchema.getSchemaID());
		c.createStatement().executeUpdate("update maxwell.columns set column_length = NULL where name = 'badcol'");

		SchemaStoreSchema.upgradeSchemaStoreSchema(c);
		Schema schemaBefore = MysqlSavedSchema.restoreFromSchemaID(this.savedSchema.getSchemaID(), c, context.getCaseSensitivity()).getSchema();
		DateTimeColumnDef cd1 = (DateTimeColumnDef) schemaBefore.findDatabase("shard_1").findTable("without_col_length").findColumn("badcol");
		assertEquals((Long) 0L, (Long) cd1.getColumnLength());

		MysqlSavedSchema restored = MysqlSavedSchema.restore(context, context.getInitialPosition());
		DateTimeColumnDef cd = (DateTimeColumnDef) restored.getSchema().findDatabase("shard_1").findTable("without_col_length").findColumn("badcol");

		assertEquals((Long) 3L, (Long) cd.getColumnLength());
	}

	@Test
	public void testTableWithSameNameInTwoDBs() throws Exception {
		String sql[] = {
			"create database dd1",
			"create database dd2",
			"create TABLE dd1.t1( i int )",
			"create TABLE dd2.t1( i int )",

		};

		server.executeList(sql);
		this.schema = new SchemaCapturer(server.getConnection(), context.getCaseSensitivity()).capture();
		this.savedSchema = new MysqlSavedSchema(this.context, this.schema, position);

		Connection c = context.getMaxwellConnection();
		this.savedSchema.save(c);

		MysqlSavedSchema restoredSchema = MysqlSavedSchema.restore(context, context.getInitialPosition());

		Table t = restoredSchema.getSchema().findDatabase("dd2").findTable("t1");
		assertThat(t, not(nullValue()));
	}


	private Schema buildSchema() {
		String charset = Charset.defaultCharset().toString();
		List<Database> databases = new ArrayList<>();
		return new Schema(databases, charset, caseSensitivity);
	}

	private void populateSchemasSurroundingTarget(
			Connection c,
			Long serverId,
			Position targetPosition,
			Long lastHeartbeat,
			String previousFile,
			String newerFile
	) throws SQLException {

		BinlogPosition targetBinlog = targetPosition.getBinlogPosition();

		// newer binlog file
		new MysqlSavedSchema(
			serverId, caseSensitivity,
			buildSchema(),
			makePosition(targetBinlog.getOffset() - 100L, newerFile, lastHeartbeat)
		).saveSchema(c);

		// newer binlog position
		new MysqlSavedSchema(
			serverId, caseSensitivity,
			buildSchema(),
			makePosition(targetBinlog.getOffset() + 100L, targetBinlog.getFile(), lastHeartbeat)
		).saveSchema(c);

		// different server ID
		new MysqlSavedSchema(
			serverId + 1L, caseSensitivity,
			buildSchema(),
			targetPosition
		).saveSchema(c);

		// older binlog file
		new MysqlSavedSchema(
			serverId, caseSensitivity,
			buildSchema(),
			makePosition(targetBinlog.getOffset(), previousFile, lastHeartbeat)
		).saveSchema(c);
	}

	private Position makePosition(long position, String file, Long lastHeartbeat) {
		return new Position(new BinlogPosition(position, file), lastHeartbeat);
	}

	@Test
	public void testFindSchemaReturnsTheLatestSchemaForTheCurrentBinlog() throws Exception {
		if (context.getConfig().gtidMode) {
			return;
		}

		Connection c = context.getMaxwellConnection();

		long serverId = 100;
		long targetPosition = 500;
		long lastHeartbeat = 9000L;
		String targetFile = "binlog08";
		String previousFile = "binlog07";
		String newerFile = "binlog09";
		Position targetBinlogPosition = makePosition(targetPosition, targetFile, lastHeartbeat + 10L);

		MysqlSavedSchema expectedSchema = new MysqlSavedSchema(serverId, caseSensitivity,
			buildSchema(),
			makePosition(targetPosition - 50L, targetFile, lastHeartbeat)
		);
		expectedSchema.save(c);

		// older binlog position
		new MysqlSavedSchema(
			serverId, caseSensitivity,
			buildSchema(),
			makePosition(targetPosition - 200L, targetFile, lastHeartbeat)
		).saveSchema(c);

		// Newer binlog position but older heartbeat
		new MysqlSavedSchema(
				serverId, caseSensitivity,
				buildSchema(),
				makePosition(targetPosition - 1L, targetFile, lastHeartbeat - 1000L)
		).saveSchema(c);

		populateSchemasSurroundingTarget(c, serverId, targetBinlogPosition, lastHeartbeat, previousFile, newerFile);

		MysqlSavedSchema foundSchema = MysqlSavedSchema.restore(context.getMaxwellConnectionPool(),
			serverId, caseSensitivity, targetBinlogPosition);
		assertThat(foundSchema.getBinlogPosition(), equalTo(expectedSchema.getBinlogPosition()));
		assertThat(foundSchema.getSchemaID(), equalTo(expectedSchema.getSchemaID()));
	}

	@Test
	public void testFindSchemaReturnsTheLatestSchemaForPreviousBinlog() throws Exception {
		if (context.getConfig().gtidMode) {
			return;
		}

		Connection c = context.getMaxwellConnection();
		long serverId = 100;
		long targetPosition = 500;
		long lastHeartbeat = 9000L;
		String targetFile = "binlog08";
		String previousFile = "binlog07";
		String newerFile = "binlog09";
		Position targetBinlogPosition = makePosition(targetPosition, targetFile, lastHeartbeat + 10L);

		// the newest schema:
		MysqlSavedSchema expectedSchema = new MysqlSavedSchema(serverId, caseSensitivity,
				buildSchema(),
				makePosition(targetPosition + 50L, previousFile, lastHeartbeat)
		);
		expectedSchema.save(c);

		// Newer binlog position but older heartbeat
		new MysqlSavedSchema(
				serverId, caseSensitivity,
				buildSchema(),
				makePosition(targetPosition - 1L, targetFile, lastHeartbeat - 1000L)
		).saveSchema(c);

		populateSchemasSurroundingTarget(c, serverId, targetBinlogPosition, lastHeartbeat, previousFile, newerFile);

		MysqlSavedSchema foundSchema = MysqlSavedSchema.restore(context.getMaxwellConnectionPool(),
			serverId, caseSensitivity, targetBinlogPosition);
		assertThat(foundSchema.getBinlogPosition(), equalTo(expectedSchema.getBinlogPosition()));
		assertThat(foundSchema.getSchemaID(), equalTo(expectedSchema.getSchemaID()));
	}
}
