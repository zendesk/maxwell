package com.zendesk.maxwell.schema.columndef;

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

import com.zendesk.maxwell.TestWithNameLogging;
import com.zendesk.maxwell.row.RawJSONString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ColumnDefTest extends TestWithNameLogging {
	private ColumnDef build(String type, boolean signed) {
		return ColumnDef.build("bar", "", type, 1, signed, null, null);
	}

	private ColumnDef build(String type, boolean signed, Long columnLength) {
		return ColumnDef.build("bar", "", type, 1, signed, null, columnLength);
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
		ColumnDef d = ColumnDef.build("bar", "utf8", "varchar", 1, false, null, null);

		assertThat(d, instanceOf(StringColumnDef.class));
		byte input[] = "He∆˚ß∆".getBytes();
		assertThat(d.toSQL(input), is("'He∆˚ß∆'"));
	}

	@Test
	public void TestUTF8MB4String() {
		String utf8_4 = "😁";

		ColumnDef d = ColumnDef.build("bar", "utf8mb4", "varchar", 1, false, null, null);
		byte input[] = utf8_4.getBytes();
		assertThat(d.toSQL(input), is("'😁'"));
	}

	@Test
	public void TestAsciiString() {
		byte input[] = new byte[] { (byte) 126, (byte) 126, (byte) 126, (byte) 126 };

		ColumnDef d = ColumnDef.build("bar", "ascii", "varchar", 1, false, null, null);
		assertThat((String) d.asJSON(input), is("~~~~"));
	}

	@Test
	public void TestStringAsJSON() {
		byte input[] = new byte[] { (byte) 169, (byte) 169, (byte) 169, (byte) 169 };

		ColumnDef d = ColumnDef.build("bar", "latin1", "varchar", 1, false, null, null);

		assertThat((String) d.asJSON(input), is("©©©©"));
	}

	@Test
	public void TestJSON() {
		byte input[] = new byte[] { (byte) 0, (byte) 1, (byte) 0, (byte) 13, (byte) 0, (byte) 11,
				(byte) 0, (byte) 2, (byte) 0, (byte) 5, (byte) 3, (byte) 0, (byte) 105, (byte) 100 };

		ColumnDef d = ColumnDef.build("bar", "ascii", "json", 1, false, null, null);

		RawJSONString result = (RawJSONString) d.asJSON(input);
		assertThat(result.json, is("{\"id\":3}"));
	}

	@Test
	public void TestEmptyJSON() {
		byte input[] = new byte[0];

		ColumnDef d = ColumnDef.build("bar", "ascii", "json", 1, false, null, null);

		RawJSONString result = (RawJSONString) d.asJSON(input);
		assertThat(result.json, is("null"));
	}

	@Test
	public void TestFloat() {
		ColumnDef d = build("float", true);
		assertThat(d, instanceOf(FloatColumnDef.class));

		assertThat(d.toSQL(Float.valueOf(1.2f)), is("1.2"));
	}

	@Test
	public void TestDouble() {
		ColumnDef d = build("double", true);
		assertThat(d, instanceOf(FloatColumnDef.class));

		String maxDouble = Double.valueOf(Double.MAX_VALUE).toString();
		assertThat(d.toSQL(Double.valueOf(Double.MAX_VALUE)), is(maxDouble));
	}

	@Test
	public void TestTime() throws ParseException {
		ColumnDef d = build("time", true);
		assertThat(d, instanceOf(TimeColumnDef.class));

		Timestamp t = new Timestamp(307653559000L - TimeZone.getDefault().getOffset(307653559000L));
		assertThat(d.toSQL(t), is("'19:19:19'"));
	}

	@Test
	public void TestTimeWithMillisecTimestamp() throws ParseException {
		ColumnDef d = build("time", true, 3L);
		assertThat(d, instanceOf(TimeColumnDef.class));

		Timestamp t = new Timestamp(307653559000L - TimeZone.getDefault().getOffset(307653559000L));

		t.setNanos(0);
		assertThat(d.toSQL(t), is("'19:19:19.000'"));

		t.setNanos(123000);
		assertThat(d.toSQL(t), is("'19:19:19.000'"));

		t.setNanos(123456789);
		assertThat(d.toSQL(t), is("'19:19:19.123'"));
	}

	@Test
	public void TestTimeWithMicrosecTimestamp() throws ParseException {
		ColumnDef d = build("time", true, 6L);
		assertThat(d, instanceOf(TimeColumnDef.class));

		Timestamp t = new Timestamp(307653559000L - TimeZone.getDefault().getOffset(307653559000L));

		t.setNanos(0);
		assertThat(d.toSQL(t), is("'19:19:19.000000'"));

		t.setNanos(123456789);
		assertThat(d.toSQL(t), is("'19:19:19.123456'"));

		t.setNanos(123000);
		assertThat(d.toSQL(t), is("'19:19:19.000123'"));
	}

	@Test
	public void TestDate() {
		ColumnDef d = build("date", true);
		assertThat(d, instanceOf(DateColumnDef.class));

		Date date = new GregorianCalendar(1979, 10, 1).getTime();
		assertThat(d.toSQL(date), is("'1979-11-01'"));
	}

	@Test
	public void TestDateTime() throws ParseException {
		ColumnDef d = build("datetime", true);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = simpleDateFormat.parse("1979-10-01 19:19:19");
		assertThat(d.toSQL(date), is("'1979-10-01 19:19:19'"));
	}

	@Test
	public void TestDateTimeWithTimestamp() throws ParseException {
		ColumnDef d = build("datetime", true);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		Timestamp t = Timestamp.valueOf("1979-10-01 19:19:19");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19'"));
	}

	@Test
	public void TestDateTimeWithMillisecprecision() throws ParseException {
		ColumnDef d = build("datetime", true, 3L);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		Timestamp t = Timestamp.valueOf("1979-10-01 19:19:19.123");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.123'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.000'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.001");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.001'"));
	}

	@Test
	public void TestDateTimeWithMicroPrecision() throws ParseException {
		ColumnDef d = build("datetime", true, 6L);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		Timestamp t = Timestamp.valueOf("1979-10-01 19:19:19.001000");
		org.junit.Assert.assertEquals(1000000, t.getNanos());
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.001000'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.000001");
		org.junit.Assert.assertEquals(1000, t.getNanos());
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.000001'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.345678");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.345678'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.100000");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.100000'"));
	}

	@Test
	public void TestTimestamp() throws ParseException {
		ColumnDef d = build("timestamp", true);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		Timestamp t = Timestamp.valueOf("1979-10-01 19:19:19");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19'"));
	}

	@Test
	public void TestTimestampWithMilliSecPrecision() throws ParseException {
		ColumnDef d = build("timestamp", true, 3L);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		Timestamp t = Timestamp.valueOf("1979-10-01 19:19:19.123456000");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.123'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.000'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.000");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.000'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.001");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.001'"));
	}

	@Test
	public void TestTimestampWithMicroSecPrecision() throws ParseException {
		ColumnDef d = build("timestamp", true, 6L);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		Timestamp t = Timestamp.valueOf("1979-10-01 19:19:19.123456000");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.123456'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.000123");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.000123'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.000001");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.000001'"));
	}

	@Test
	public void TestBit() {
		ColumnDef d = build("bit", true);
		assertThat(d, instanceOf(BitColumnDef.class));

		byte[] b = new byte[]{0x1};
		assertThat(d.toSQL(b), is("1"));

		b = new byte[]{0x0};
		assertThat(d.toSQL(b), is("0"));

		Boolean bO = Boolean.TRUE;
		assertThat(d.toSQL(bO), is("1"));

		bO = Boolean.FALSE;
		assertThat(d.toSQL(bO), is("0"));
	}

}
