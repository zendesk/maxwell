package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.schema.columndef.BigIntColumnDef;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.DateColumnDef;
import com.zendesk.maxwell.schema.columndef.DateTimeColumnDef;
import com.zendesk.maxwell.schema.columndef.FloatColumnDef;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

public class ColumnDefTest {
	private ColumnDef build(String type, boolean signed) {
		return ColumnDef.build("foo", "bar", "", type, 1, signed, null);
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTinyInt() {
		ColumnDef d = build("tinyint", true);

		assertThat(d, instanceOf(IntColumnDef.class));
		assertThat(d.toSQL(Integer.valueOf(5)), is("5"));
		assertThat(d.toSQL(Integer.valueOf(-5)), is("-5"));

		d = build("tinyint", false);
		assertThat(d.toSQL(Integer.valueOf(10)), is("10"));
		assertThat(d.toSQL(Integer.valueOf(-10)), is("246"));
	}

	@Test
	public void testShortInt() {
		ColumnDef d = build("smallint", true);

		assertThat(d, instanceOf(IntColumnDef.class));
		assertThat(d.toSQL(Integer.valueOf(5)), is("5"));
		assertThat(d.toSQL(Integer.valueOf(-5)), is("-5"));

		d = build("smallint", false);
		assertThat(d.toSQL(Integer.valueOf(-10)), is("65526"));
	}

	@Test
	public void testMediumInt() {
		ColumnDef d = build("mediumint", true);

		assertThat(d, instanceOf(IntColumnDef.class));
		assertThat(d.toSQL(Integer.valueOf(5)), is("5"));
		assertThat(d.toSQL(Integer.valueOf(-5)), is("-5"));

		d = build("mediumint", false);
		assertThat(d.toSQL(Integer.valueOf(-10)), is("16777206"));

	}

	@Test
	public void testInt() {
		ColumnDef d = build("int", true);

		assertThat(d, instanceOf(IntColumnDef.class));
		assertThat(d.toSQL(Integer.valueOf(5)), is("5"));
		assertThat(d.toSQL(Integer.valueOf(-5)), is("-5"));

		d = build("int", false);
		assertThat(d.toSQL(Integer.valueOf(-10)), is("4294967286"));
	}

	@Test
	public void testBigInt() {
		ColumnDef d = build("bigint", true);

		assertThat(d, instanceOf(BigIntColumnDef.class));
		assertThat(d.toSQL(Long.valueOf(5)), is("5"));
		assertThat(d.toSQL(Long.valueOf(-5)), is("-5"));

		d = build("bigint", false);
		assertThat(d.toSQL(Long.valueOf(-10)), is("18446744073709551606"));
	}

	@Test
	public void testUTF8String() {
		ColumnDef d = ColumnDef.build("foo", "bar", "utf8", "varchar", 1, false, null);

		assertThat(d, instanceOf(StringColumnDef.class));
		byte input[] = "He∆˚ß∆".getBytes();
		assertThat(d.toSQL(input), is("'He∆˚ß∆'"));
	}

	@Test
	public void TestUTF8MB4String() {
		String utf8_4 = "😁";

		ColumnDef d = ColumnDef.build("foo", "bar", "utf8mb4", "varchar", 1, false, null);
		byte input[] = utf8_4.getBytes();
		assertThat(d.toSQL(input), is("'😁'"));
	}

	@Test
	public void TestAsciiString() {
		byte input[] = new byte[4];
		input[0] = Byte.valueOf((byte) 126);
		input[1] = Byte.valueOf((byte) 126);
		input[2] = Byte.valueOf((byte) 126);
		input[3] = Byte.valueOf((byte) 126);

		ColumnDef d = ColumnDef.build("foo", "bar", "ascii", "varchar", 1, false, null);
		assertThat((String) d.asJSON(input), is("~~~~"));
	}

	@Test
	public void TestStringAsJSON() {
		byte input[] = new byte[4];
		input[0] = Byte.valueOf((byte) 169);
		input[1] = Byte.valueOf((byte) 169);
		input[2] = Byte.valueOf((byte) 169);
		input[3] = Byte.valueOf((byte) 169);

		ColumnDef d = ColumnDef.build("foo", "bar", "latin1", "varchar", 1, false, null);

		assertThat((String) d.asJSON(input), is("©©©©"));
	}

	@Test
	public void TestFloat() {
		ColumnDef d = build("float", true);
		assertThat(d, instanceOf(FloatColumnDef.class));

		assertTrue(d.matchesMysqlType(MySQLConstants.TYPE_FLOAT));
		assertFalse(d.matchesMysqlType(MySQLConstants.TYPE_DOUBLE));

		assertThat(d.toSQL(Float.valueOf(1.2f)), is("1.2"));
	}

	@Test
	public void TestDouble() {
		ColumnDef d = build("double", true);
		assertThat(d, instanceOf(FloatColumnDef.class));

		assertTrue(d.matchesMysqlType(MySQLConstants.TYPE_DOUBLE));
		assertFalse(d.matchesMysqlType(MySQLConstants.TYPE_FLOAT));

		String maxDouble = Double.valueOf(Double.MAX_VALUE).toString();
		assertThat(d.toSQL(Double.valueOf(Double.MAX_VALUE)), is(maxDouble));
	}

	@Test
	public void TestDate() {
		ColumnDef d = build("date", true);
		assertThat(d, instanceOf(DateColumnDef.class));

		assertTrue(d.matchesMysqlType(MySQLConstants.TYPE_DATE));

		Date date = new GregorianCalendar(1979, 10, 1).getTime();
		assertThat(d.toSQL(date), is("'1979-11-01'"));
	}

	@Test
	public void TestDateTime() throws ParseException {
		ColumnDef d = build("datetime", true);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		assertTrue(d.matchesMysqlType(MySQLConstants.TYPE_DATETIME));

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = simpleDateFormat.parse("1979-10-01 19:19:19");
		assertThat(d.toSQL(date), is("'1979-10-01 19:19:19'"));
	}

	@Test
	public void TestTimestamp() throws ParseException {
		ColumnDef d = build("timestamp", true);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		assertTrue(d.matchesMysqlType(MySQLConstants.TYPE_TIMESTAMP));

		Timestamp t = new Timestamp(307653559000L);
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19'"));
	}

}
