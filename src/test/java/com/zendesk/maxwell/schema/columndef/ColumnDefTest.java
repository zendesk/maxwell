package com.zendesk.maxwell.schema.columndef;

import com.google.common.collect.ImmutableList;
import com.zendesk.maxwell.TestWithNameLogging;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.RawJSONString;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ColumnDefTest extends TestWithNameLogging {
	private final List<Class<?>> ALL_CLASSES = ImmutableList.<Class<?>>builder()
			.add(BigIntColumnDef.class)
			.add(BitColumnDef.class)
			.add(ColumnDef.class)
			.add(ColumnDefWithLength.class)
			.add(DateColumnDef.class)
			.add(DateTimeColumnDef.class)
			.add(DecimalColumnDef.class)
			.add(EnumColumnDef.class)
			.add(EnumeratedColumnDef.class)
			.add(FloatColumnDef.class)
			.add(GeometryColumnDef.class)
			.add(IntColumnDef.class)
			.add(JsonColumnDef.class)
			.add(SetColumnDef.class)
			.add(StringColumnDef.class)
			.add(TimeColumnDef.class)
			.add(YearColumnDef.class)
			.build();

	private ColumnDef build(String type, boolean signed) {
		return ColumnDef.build("bar", "", type, (short) 1, signed, null, null);
	}

	private ColumnDef build(String type, boolean signed, Long columnLength) {
		return ColumnDef.build("bar", "", type, (short) 1, signed, null, columnLength);
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTinyInt() throws Exception {
		ColumnDef d = build("tinyint", true);

		assertThat(d, instanceOf(IntColumnDef.class));
		assertThat(d.toSQL(Integer.valueOf(5)), is("5"));
		assertThat(d.toSQL(Integer.valueOf(-5)), is("-5"));

		d = build("tinyint", false);
		assertThat(d.toSQL(Integer.valueOf(10)), is("10"));
		assertThat(d.toSQL(Integer.valueOf(-10)), is("246"));
	}

	@Test
	public void testShortInt() throws Exception {
		ColumnDef d = build("smallint", true);

		assertThat(d, instanceOf(IntColumnDef.class));
		assertThat(d.toSQL(Integer.valueOf(5)), is("5"));
		assertThat(d.toSQL(Integer.valueOf(-5)), is("-5"));

		d = build("smallint", false);
		assertThat(d.toSQL(Integer.valueOf(-10)), is("65526"));
	}

	@Test
	public void testMediumInt() throws Exception {
		ColumnDef d = build("mediumint", true);

		assertThat(d, instanceOf(IntColumnDef.class));
		assertThat(d.toSQL(Integer.valueOf(5)), is("5"));
		assertThat(d.toSQL(Integer.valueOf(-5)), is("-5"));

		d = build("mediumint", false);
		assertThat(d.toSQL(Integer.valueOf(-10)), is("16777206"));

	}

	@Test
	public void testInt() throws Exception {
		ColumnDef d = build("int", true);

		assertThat(d, instanceOf(IntColumnDef.class));
		assertThat(d.toSQL(Integer.valueOf(5)), is("5"));
		assertThat(d.toSQL(Integer.valueOf(-5)), is("-5"));

		d = build("int", false);
		assertThat(d.toSQL(Integer.valueOf(-10)), is("4294967286"));
	}

	@Test
	public void testBigInt() throws ColumnDefCastException {
		ColumnDef d = build("bigint", true);

		assertThat(d, instanceOf(BigIntColumnDef.class));
		assertThat(d.toSQL(Long.valueOf(5)), is("5"));
		assertThat(d.toSQL(Long.valueOf(-5)), is("-5"));

		d = build("bigint", false);
		assertThat(d.toSQL(Long.valueOf(-10)), is("18446744073709551606"));
	}

	@Test
	public void testUTF8String() throws ColumnDefCastException {
		ColumnDef d = ColumnDef.build("bar", "utf8", "varchar", (short) 1, false, null, null);

		assertThat(d, instanceOf(StringColumnDef.class));
		byte input[] = "He∆˚ß∆".getBytes();
		assertThat(d.toSQL(input), is("'He∆˚ß∆'"));
	}

	@Test
	public void TestUTF8MB4String() throws ColumnDefCastException {
		String utf8_4 = "😁";

		ColumnDef d = ColumnDef.build("bar", "utf8mb4", "varchar", (short) 1, false, null, null);
		byte input[] = utf8_4.getBytes();
		assertThat(d.toSQL(input), is("'😁'"));
	}

	@Test
	public void TestAsciiString() throws ColumnDefCastException {
		byte input[] = new byte[] { (byte) 126, (byte) 126, (byte) 126, (byte) 126 };

		ColumnDef d = ColumnDef.build("bar", "ascii", "varchar", (short) 1, false, null, null);
		assertThat((String) d.asJSON(input, null), is("~~~~"));
	}

	@Test
	public void TestLatin1String() throws ColumnDefCastException {
		byte input[] = new byte[] { (byte) 128, (byte) 128, (byte) 128, (byte) 128 };

		ColumnDef d = ColumnDef.build("bar", "latin1", "varchar", (short) 1, false, null, null);
		assertThat((String) d.asJSON(input, null), is("€€€€"));
	}

	@Test
	public void TestStringAsJSON() throws ColumnDefCastException {
		byte input[] = new byte[] { (byte) 169, (byte) 169, (byte) 169, (byte) 169 };

		ColumnDef d = ColumnDef.build("bar", "latin1", "varchar", (short) 1, false, null, null);

		assertThat((String) d.asJSON(input, null), is("©©©©"));
	}

	@Test
	public void TestJSON() throws ColumnDefCastException {
		byte input[] = new byte[] { (byte) 0, (byte) 1, (byte) 0, (byte) 13, (byte) 0, (byte) 11,
				(byte) 0, (byte) 2, (byte) 0, (byte) 5, (byte) 3, (byte) 0, (byte) 105, (byte) 100 };

		ColumnDef d = ColumnDef.build("bar", "ascii", "json", (short) 1, false, null, null);

		RawJSONString result = (RawJSONString) d.asJSON(input, null);
		assertThat(result.json, is("{\"id\":3}"));
	}

	@Test
	public void TestEmptyJSON() throws ColumnDefCastException {
		byte input[] = new byte[0];

		ColumnDef d = ColumnDef.build("bar", "ascii", "json", (short) 1, false, null, null);

		RawJSONString result = (RawJSONString) d.asJSON(input, null);
		assertThat(result.json, is("null"));
	}

	@Test
	public void TestFloat() throws ColumnDefCastException {
		ColumnDef d = build("float", true);
		assertThat(d, instanceOf(FloatColumnDef.class));

		assertThat(d.toSQL(Float.valueOf(1.2f)), is("1.2"));
	}

	@Test
	public void TestDouble() throws ColumnDefCastException {
		ColumnDef d = build("double", true);
		assertThat(d, instanceOf(FloatColumnDef.class));

		String maxDouble = Double.valueOf(Double.MAX_VALUE).toString();
		assertThat(d.toSQL(Double.valueOf(Double.MAX_VALUE)), is(maxDouble));
	}

	@Test
	public void TestTime() throws ParseException, ColumnDefCastException {
		ColumnDef d = build("time", true);
		assertThat(d, instanceOf(TimeColumnDef.class));

		Timestamp t = new Timestamp(307653559000L - TimeZone.getDefault().getOffset(307653559000L));
		assertThat(d.toSQL(t), is("'19:19:19'"));
	}

	@Test
	public void TestTimeWithMillisecTimestamp() throws ParseException, ColumnDefCastException {
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
	public void TestTimeWithMicrosecTimestamp() throws ParseException, ColumnDefCastException {
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
	public void TestDate() throws ColumnDefCastException {
		ColumnDef d = build("date", true);
		assertThat(d, instanceOf(DateColumnDef.class));

		Date date = new GregorianCalendar(1979, 10, 1).getTime();
		assertThat(d.toSQL(date), is("'1979-11-01'"));
	}

	@Test
	public void TestDateZeroDates() throws ColumnDefCastException {
		ColumnDef d = build("date", true);

		MaxwellOutputConfig config = new MaxwellOutputConfig();
		config.zeroDatesAsNull = true;

		assertEquals(null, d.asJSON(Long.MIN_VALUE, config));
	}


	@Test
	public void TestDateTime() throws ParseException, ColumnDefCastException {
		ColumnDef d = build("datetime", true);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = simpleDateFormat.parse("1979-10-01 19:19:19");
		assertThat(d.toSQL(date), is("'1979-10-01 19:19:19'"));
	}

	@Test
	public void TestDateTimeZeroDates() throws ColumnDefCastException {
		ColumnDef d = build("datetime", true);

		MaxwellOutputConfig config = new MaxwellOutputConfig();
		config.zeroDatesAsNull = true;

		assertEquals(null, d.asJSON(Long.MIN_VALUE, config));
	}

	@Test
	public void TestDateTimeWithTimestamp() throws ParseException, ColumnDefCastException {
		ColumnDef d = build("datetime", true);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		Timestamp t = Timestamp.valueOf("1979-10-01 19:19:19");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19'"));
	}

	@Test
	public void TestDateTimeWithMillisecprecision() throws ParseException, ColumnDefCastException {
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
	public void TestDateTimeWithMicroPrecision() throws ParseException, ColumnDefCastException {
		ColumnDef d = build("datetime", true, 6L);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		Timestamp t = Timestamp.valueOf("1979-10-01 19:19:19.001000");
		assertEquals(1000000, t.getNanos());
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.001000'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.000001");
		assertEquals(1000, t.getNanos());
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.000001'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.345678");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.345678'"));

		t = Timestamp.valueOf("1979-10-01 19:19:19.100000");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19.100000'"));
	}

	@Test
	public void TestTimestamp() throws ParseException, ColumnDefCastException {
		ColumnDef d = build("timestamp", true);
		assertThat(d, instanceOf(DateTimeColumnDef.class));

		Timestamp t = Timestamp.valueOf("1979-10-01 19:19:19");
		assertThat(d.toSQL(t), is("'1979-10-01 19:19:19'"));
	}

	@Test
	public void TestTimestampWithMilliSecPrecision() throws ParseException, ColumnDefCastException {
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
	public void TestTimestampWithMicroSecPrecision() throws ParseException, ColumnDefCastException {
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
	public void TestBit() throws ColumnDefCastException {
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

	@Test
	public void testEquality() throws ColumnDefCastException {
		ColumnDef a = ColumnDef.build("bar", "utf8", "varchar", (short) 1, false, null, null);
		ColumnDef b = ColumnDef.build("bar", "utf8", "varchar", (short) 1, false, null, null);
		Assert.assertTrue(a == b); // verify auto-interning works
	}

	@Test
	public void testWithPos() throws ColumnDefCastException {
		ColumnDef a = ColumnDef.build("bar", "utf8", "varchar", (short) 1, false, null, null);
		ColumnDef b = ColumnDef.build("bar", "utf8", "varchar", (short) 2, false, null, null);
		ColumnDef aToB = a.withPos((short) 2);
		ColumnDef bToA = b.withPos((short) 1);
		Assert.assertTrue(a.getPos() == 1);
		Assert.assertTrue(b.getPos() == 2);
		Assert.assertTrue(a == bToA); // verify auto-interning works
		Assert.assertTrue(b == aToB); // verify auto-interning works
	}

	@Test
	public void testWithName() throws ColumnDefCastException {
		ColumnDef a = ColumnDef.build("foo", "utf8", "varchar", (short) 1, false, null, null);
		ColumnDef b = ColumnDef.build("bar", "utf8", "varchar", (short) 1, false, null, null);
		ColumnDef aToB = a.withName("bar");
		ColumnDef bToA = b.withName("foo");
		Assert.assertEquals("foo", a.getName());
		Assert.assertEquals("bar", b.getName());
		Assert.assertTrue(a == bToA); // verify auto-interning works
		Assert.assertTrue(b == aToB); // verify auto-interning works
	}

	/**
	 * Series of checks that attempt to detect a modification that will introduce mutability into ColumnDefs. This isn't
	 * perfect, it won't detect exposing a mutable List, but will catch common errors in the rare cases someone adds a
	 * type
	 */
	@Test
	public void testInterfaceImmutability() {
		// not sure how to use testng equivalent of dataprovider with junit
		// all ColumnDef classes should be listed
		for (Class<?> clazz : ALL_CLASSES) {
			List<Class<?>> classesToTest = new ArrayList<>();
			classesToTest.add(clazz);
			Class<?> nextClazz = clazz.getSuperclass();
			while (nextClazz != null && nextClazz != Object.class) {
				classesToTest.add(nextClazz);
				nextClazz = nextClazz.getSuperclass();
			}
			for (Class<?> cut : classesToTest) {
				Field[] declaredFields = cut.getDeclaredFields();
				String className = cut.getName();

				// check for setters
				boolean foundEquals = false;
				boolean foundHashCode = false;
				for (Method m : cut.getDeclaredMethods()) {
					if (m.getName().startsWith("set")) {
						Assert.fail(className + ": All methods extending ColumnDef must be immutable so hashCode/equals work. Use withXXX and clone object instead of setting values");
					} else if (m.getName().equals("equals") && m.getParameterCount() == 1 && m.getParameterTypes()[0] == Object.class) {
						foundEquals = true;
					} else if (m.getName().equals("hashCode") && m.getParameterCount() == 0) {
						foundHashCode = true;
					}
				}

				for (Field field : declaredFields) {
					final int modifiers = field.getModifiers();
					final boolean isStatic = Modifier.isStatic(modifiers);
					if (isStatic) {
						// assume static is fine
						continue;
					}

					final boolean isFinal = Modifier.isFinal(modifiers);
					final boolean isPrivate = Modifier.isPrivate(modifiers);

					// check field immutability
					if (!isFinal && !isPrivate) {
						Assert.fail(className + ": Non-private field " + field.getName() + " is mutable. All classes must have immutable interfaces (public, protected, package-private)");
					}
					if (!isPrivate) {
						Assert.fail(className + ": Should not have direct access to member variable " + field.getName());
					}

					if (!(foundEquals && foundHashCode)) {
						if (cut == DateTimeColumnDef.class && field.getName().equals("isTimestamp")) {
							// this field is derived from type, there's no need for equals/hashCode with this variable
						} else {
							Assert.fail(className + " has member variables, but not equals/HashCode implementations");
						}
					}
				}
			}
		}
	}


	@Test
	public void testConstructorDefinitions() {
		for (Class<?> clazz : ALL_CLASSES) {
			if (Modifier.isAbstract(clazz.getModifiers())) {
				continue;
			}
			for (Constructor c : clazz.getDeclaredConstructors()) {
				Assert.assertTrue(clazz.getName() + ": ColumnDef concrete constructors should all be private and have " +
						"a create method to intern new creations", Modifier.isPrivate(c.getModifiers()));
			}
			boolean foundCreate = false;
			for (Method m : clazz.getDeclaredMethods()) {
				if ("create".equals(m.getName())) {
					foundCreate = true;
					Assert.assertTrue(clazz.getName() + ": create method must be static", Modifier.isStatic(m.getModifiers()));
				}
			}
			Assert.assertTrue(clazz.getName() + ": all ColumnDef methods have a create method to intern instances", foundCreate);
		}
	}

}
