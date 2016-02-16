package com.zendesk.maxwell;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AbstractIntegrationTest extends AbstractMaxwellTest {
	public static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_REF = new TypeReference<Map<String, Object>>() {};

	ObjectMapper mapper = new ObjectMapper();
	protected Map<String, Object> parseJSON(String json) throws Exception {
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		return mapper.readValue(json, MaxwellIntegrationTest.MAP_STRING_OBJECT_REF);
	}

	protected void assertJSON(List<Map<String, Object>> jsonOutput, List<Map<String, Object>> jsonAsserts) {
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

	private void runJSONTest(List<String> sql, List<Map<String, Object>> expectedJSON) throws Exception {
		List<Map<String, Object>> eventJSON = new ArrayList<>();
		List<Map<String, Object>> matched = new ArrayList<>();
		List<RowMap> rows = getRowsForSQL(null, sql.toArray(new String[sql.size()]));

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

	protected class SQLAndJSON {
		public ArrayList<Map<String, Object>> jsonAsserts;
		public ArrayList<String> inputSQL;

		protected SQLAndJSON() {
			this.jsonAsserts = new ArrayList<>();
			this.inputSQL = new ArrayList<>();
		}
	}

	protected SQLAndJSON parseJSONTestFile(String fname) throws Exception {
		SQLAndJSON ret = new SQLAndJSON();
		File file = new File(fname);
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
				if (line.matches("^\\s+.*$")) { // leading whitespace -- continuation of previous line
					buffer = buffer + " " + line.trim();
				} else {
					if ( bufferIsJSON )	{
						ret.jsonAsserts.add(mapper.<Map<String, Object>>readValue(buffer, MaxwellIntegrationTest.MAP_STRING_OBJECT_REF));
					} else {
						ret.inputSQL.add(buffer);
					}
					buffer = null;
				}
			}

			if ( buffer == null ) {
				if ( line.matches("^\\s*\\->\\s*\\{.*") ) {
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
				ret.jsonAsserts.add(mapper.<Map<String, Object>>readValue(buffer, MaxwellIntegrationTest.MAP_STRING_OBJECT_REF));
			} else {
				ret.inputSQL.add(buffer);
			}
		}

		reader.close();
		return ret;
	}

	protected void runJSONTestFile(String fname) throws Exception {
		SQLAndJSON testResources = parseJSONTestFile(fname);
	    runJSONTest(testResources.inputSQL, testResources.jsonAsserts);
	}
}
