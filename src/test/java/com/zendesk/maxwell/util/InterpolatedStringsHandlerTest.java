package com.zendesk.maxwell.util;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowIdentity;
import com.zendesk.maxwell.row.RowMap;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class InterpolatedStringsHandlerTest {

	private static final String DATABASE_TEMPLATE = "%{database}";

	private static final String DATABASE_TABLE_TEMPLATE = DATABASE_TEMPLATE + ".%{table}";

	private static final String DATABASE_TABLE_TYPE_TEMPLATE = DATABASE_TABLE_TEMPLATE + ".%{type}";


	@Test
	public void doNothingIfStringIsNotInterpolated() {
		RowIdentity pk = newRowIdentity();
		RowMap r = newRowMap();

		InterpolatedStringsHandler myString = new InterpolatedStringsHandler("abcxyz");
		assertThat(myString.generateFromRowIdentity(pk), equalTo("abcxyz"));
		assertThat(myString.generateFromRowMap(r), equalTo("abcxyz"));

		InterpolatedStringsHandler emptyStr = new InterpolatedStringsHandler("");
		assertThat(emptyStr.generateFromRowIdentity(pk), equalTo(""));
		assertThat(emptyStr.generateFromRowMap(r), equalTo(""));
	}

	@Test
	public void generateFromRowIdentityCorrectly() {
		InterpolatedStringsHandler db = new InterpolatedStringsHandler(DATABASE_TEMPLATE);
		assertThat(db.generateFromRowIdentity(newRowIdentity()), equalTo("testDb"));
		assertThat(db.generateFromRowIdentity(new RowIdentity("testDb", "testTable", null, null)),
				equalTo("testDb"));
		assertThat(db.generateFromRowIdentity(new RowIdentity("testDb", null, "update", null)),
				equalTo("testDb"));
		assertThat(db.generateFromRowIdentity(new RowIdentity("testDb", null, null, null)),
				equalTo("testDb"));


		InterpolatedStringsHandler dbTable = new InterpolatedStringsHandler(DATABASE_TABLE_TEMPLATE);
		assertThat(dbTable.generateFromRowIdentity(newRowIdentity()), equalTo("testDb.testTable"));
		assertThat(dbTable.generateFromRowIdentity(new RowIdentity("testDb", "testTable", null, null)),
				equalTo("testDb.testTable"));
		assertThat(dbTable.generateFromRowIdentity(new RowIdentity("testDb", null, "update", null)),
				equalTo("testDb."));
		assertThat(dbTable.generateFromRowIdentity(new RowIdentity("testDb", null, null, null)),
				equalTo("testDb."));

	}

	@Test
	public void generateFromRowMapCorrectly() {
		InterpolatedStringsHandler db = new InterpolatedStringsHandler(DATABASE_TEMPLATE);
		assertThat(db.generateFromRowMap(newRowMap()), equalTo("testDb"));
		assertThat(db.generateFromRowMap(new RowMap("insert", "testDb", "testTable", System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb"));
		assertThat(db.generateFromRowMap(new RowMap("insert", "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb"));
		assertThat(db.generateFromRowMap(new RowMap(null, "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb"));

		InterpolatedStringsHandler dbTable = new InterpolatedStringsHandler(DATABASE_TABLE_TEMPLATE);
		assertThat(dbTable.generateFromRowMap(newRowMap()), equalTo("testDb.testTable"));
		assertThat(dbTable.generateFromRowMap(new RowMap(null, "testDb", "testTable", System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb.testTable"));
		assertThat(dbTable.generateFromRowMap(new RowMap("insert", "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb."));
		assertThat(dbTable.generateFromRowMap(new RowMap(null, "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb."));

		InterpolatedStringsHandler dbTableType = new InterpolatedStringsHandler(DATABASE_TABLE_TYPE_TEMPLATE);
		assertThat(dbTableType.generateFromRowMap(newRowMap()), equalTo("testDb.testTable.insert"));
		assertThat(dbTable.generateFromRowMap(new RowMap(null, "testDb", "testTable", System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb.testTable"));
		assertThat(dbTableType.generateFromRowMap(new RowMap("insert", "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb..insert"));
		assertThat(dbTableType.generateFromRowMap(new RowMap(null, "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb.."));
	}

	@Test
	public void generateFromRowMapCorrectlyAndTrimAllWhitesSpaces() {
		InterpolatedStringsHandler dbTable = new InterpolatedStringsHandler("  %{database} .  %{table} \n");
		assertThat(dbTable.generateFromRowMapAndTrimAllWhitesSpaces(newRowMap()), equalTo("testDb.testTable"));
		assertThat(dbTable.generateFromRowMapAndTrimAllWhitesSpaces(new RowMap(null, "testDb", "testTable", System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb.testTable"));
		assertThat(dbTable.generateFromRowMapAndTrimAllWhitesSpaces(new RowMap("insert", "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb."));
		assertThat(dbTable.generateFromRowMapAndTrimAllWhitesSpaces(new RowMap(null, "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L))),
				equalTo("testDb."));

	}


        @Test
	public void interpolateStringWithType() {
		InterpolatedStringsHandler interpolatedStringsHandler = new InterpolatedStringsHandler(DATABASE_TABLE_TYPE_TEMPLATE);

		assertThat(interpolatedStringsHandler.interpolate("testDb", "testTable", "insert"), equalTo("testDb.testTable.insert"));
	}

	@Test
	public void interpolateStringWithoutType() {
		InterpolatedStringsHandler interpolatedStringsHandler = new InterpolatedStringsHandler(DATABASE_TABLE_TEMPLATE);

		assertThat(interpolatedStringsHandler.interpolate("testDb", "testTable", null), equalTo("testDb.testTable"));
	}

	private RowIdentity newRowIdentity() {
		return new RowIdentity("testDb", "testTable", "insert", null);
	}

	private RowMap newRowMap() {
		return new RowMap("insert", "testDb", "testTable", System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L));
	}
}
