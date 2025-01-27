package com.zendesk.maxwell.scripting;

import org.junit.Test;

import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.ResolvedTableAlter;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import static org.junit.Assert.*;

public class ScriptingTest {
	private <T> T getPrivateFieldOrFail(Object obj, String fieldName) throws Exception {
		Class<?> scriptingHooked = obj.getClass();
		Field field = scriptingHooked.getDeclaredField(fieldName);
		field.setAccessible(true);
		Object value = field.get(obj);
		if (value == null) {
			fail(fieldName + " field is null");
		}

		return (T) field.get(obj);
	}
	@Test
	public void TestScripting() throws Exception {
		// Write a simple scripting file
		Scripting scripting = new Scripting("src/test/resources/scripting/test-set-state.js");

		// String type, String database, String table, Long timestampMillis, List<String> pkColumns, Position position, Position nextPosition, String rowQuery
		RowMap row = new RowMap(
			"insert", 
			"mydatabase", 
			"mytable", 
			0L, new ArrayList<String>(), 
			new Position(null, 0), 
			new Position(null, 0), 
			"SELECT 1"
		);

		scripting.invoke(row);

		// Access the private globalJavascriptState field
		LinkedHashMap<String, Object> globalJavascriptState = 
			getPrivateFieldOrFail(scripting, "globalJavascriptState");
		
		assertEquals(globalJavascriptState.get("mykey"), "myvalue");
	}

	@Test
	public void TestScriptingDDL() throws Exception {
		// Write a simple scripting file
		Scripting scripting = new Scripting("src/test/resources/scripting/test-set-state.js");

		// String type, String database, String table, Long timestampMillis, List<String> pkColumns, Position position, Position nextPosition, String rowQuery
		RowMap row = new DDLMap(
			new ResolvedTableAlter(),
			123L,
			"INSERT INTO mytable VALUES (1, 2, 3)",
			new Position(null, 0),
			new Position(null, 0),
			13L
		);

			
		// Access the private globalJavascriptState field
		LinkedHashMap<String, Object> globalJavascriptState = 
			getPrivateFieldOrFail(scripting, "globalJavascriptState");

		globalJavascriptState.put("number", "1");

		scripting.invoke(row);

		assertEquals(globalJavascriptState.get("number"), "2");
	}

	@Test
	public void DontFailIfScriptHasNoStateParameter() throws Exception {
		// Write a simple scripting file
		Scripting scripting = new Scripting("src/test/resources/scripting/test-no-state-suppress-row.js");

		// String type, String database, String table, Long timestampMillis, List<String> pkColumns, Position position, Position nextPosition, String rowQuery
		RowMap row = new RowMap(
			"insert", 
			"mydatabase", 
			"mytable", 
			0L, new ArrayList<String>(), 
			new Position(null, 0), 
			new Position(null, 0), 
			"SELECT 1"
		);

		scripting.invoke(row);

		// Access the private suppressed field
		boolean suppressed = 
			getPrivateFieldOrFail(row, "suppressed");


		assertEquals(suppressed, true);
	}
}

