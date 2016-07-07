package com.zendesk.maxwell;

import java.util.*;
import java.io.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class MaxwellTestJSON {
	/* methods around running JSON test files */
	public static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_REF = new TypeReference<Map<String, Object>>() {};

	public static Map<String, Object> parseJSON(String json) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		return mapper.readValue(json, MAP_STRING_OBJECT_REF);
	}

	public static void assertJSON(List<Map<String, Object>> jsonOutput, List<Map<String, Object>> jsonAsserts) {
		ArrayList<Map<String, Object>> missing = new ArrayList<>();

		for ( Map m : jsonAsserts ) {
			if ( !jsonOutput.contains(m) )
				missing.add(m);
		}

		if ( missing.size() > 0 ) {
			String msg = "Did not find: \n" +
					StringUtils.join(missing.iterator(), "\n") +
					"\n\n in : " +
					StringUtils.join(jsonOutput.iterator(), "\n");
			assertThat(msg, false, is(true));
		}
	}

	private static void runJSONTest(MysqlIsolatedServer server, List<String> sql, List<Map<String, Object>> expectedJSON) throws Exception {
		List<Map<String, Object>> eventJSON = new ArrayList<>();
		List<Map<String, Object>> matched = new ArrayList<>();
		List<RowMap> rows = MaxwellTestSupport.getRowsForSQL(server, null, sql.toArray(new String[sql.size()]));

		for ( RowMap r : rows ) {
			String s = r.toJSON();

			Map<String, Object> outputMap = parseJSON(s);

			outputMap.remove("ts");
			outputMap.remove("xid");
			outputMap.remove("commit");

			eventJSON.add(outputMap);
		}
		assertJSON(eventJSON, expectedJSON);

	}

	public static class SQLAndJSON {
		public ArrayList<Map<String, Object>> jsonAsserts;
		public ArrayList<String> inputSQL;

		protected SQLAndJSON() {
			this.jsonAsserts = new ArrayList<>();
			this.inputSQL = new ArrayList<>();
		}
	}

	static final String JSON_PATTERN = "^\\s*\\->\\s*\\{.*";
	public static SQLAndJSON parseJSONTestFile(String fname) throws Exception {
		File file = new File(fname);
		SQLAndJSON ret = new SQLAndJSON();
		BufferedReader reader = new BufferedReader(new FileReader(file));
		ObjectMapper mapper = new ObjectMapper();

		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

		String buffer = null;
		boolean bufferIsJSON = false;
		while ( reader.ready() ) {
			String line = reader.readLine();

			if (line.matches("^\\s*$")) { // skip blanks
				continue;
			}

			if ( buffer != null ) {
				if (line.matches("^\\s+.*$") && !line.matches(JSON_PATTERN)) { // leading whitespace -- continuation of previous line
					buffer = buffer + " " + line.trim();
				} else {
					if ( bufferIsJSON )	{
						ret.jsonAsserts.add(mapper.<Map<String, Object>>readValue(buffer, MAP_STRING_OBJECT_REF));
					} else {
						ret.inputSQL.add(buffer);
					}
					buffer = null;
				}
			}

			if ( buffer == null ) {
				if ( line.matches(JSON_PATTERN) ) {
					line = line.replaceAll("^\\s*\\->\\s*", "");
					bufferIsJSON = true;
				} else {
					bufferIsJSON = false;
				}
				buffer = line;
			}
		}

		if ( buffer != null ) {
			if ( bufferIsJSON )	{
				ret.jsonAsserts.add(mapper.<Map<String, Object>>readValue(buffer, MAP_STRING_OBJECT_REF));
			} else {
				ret.inputSQL.add(buffer);
			}
		}

		reader.close();
		return ret;
	}

	protected static void runJSONTestFile(MysqlIsolatedServer server, String dir, String fname) throws Exception {
		SQLAndJSON testResources = parseJSONTestFile(new File(dir, fname).toString());
	    runJSONTest(server, testResources.inputSQL, testResources.jsonAsserts);
	}

	protected static void runJSONTestFile(MysqlIsolatedServer server, String fname) throws Exception {
		runJSONTestFile(server, MaxwellTestSupport.getSQLDir(), fname);
	}
}

