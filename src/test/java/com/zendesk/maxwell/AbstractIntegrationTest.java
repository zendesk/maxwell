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

			for ( Map<String, Object> b : expectedJSON ) {
				if ( outputMap.equals(b) )
					matched.add(b);
			}
		}

		for ( Map j : matched ) {
			expectedJSON.remove(j);
		}

		if ( expectedJSON.size() > 0 ) {
			String msg = "Did not find: \n" +
						 StringUtils.join(expectedJSON.iterator(), "\n") +
						 "\n\n in : " +
						 StringUtils.join(eventJSON.iterator(), "\n");
			assertThat(msg, false, is(true));

		}
	}

	protected void runJSONTestFile(String fname) throws Exception {
		File file = new File(fname);
		ArrayList<Map<String, Object>> jsonAsserts = new ArrayList<>();
		ArrayList<String> inputSQL  = new ArrayList<>();
		BufferedReader reader = new BufferedReader(new FileReader(file));
		ObjectMapper mapper = new ObjectMapper();

		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

		while ( reader.ready() ) {
			String line = reader.readLine();
			if ( line.matches("^\\s*$")) {
				continue;
			}

			if ( line.matches("^\\s*\\->\\s*\\{.*") ) {
				line = line.replaceAll("^\\s*\\->\\s*", "");

				jsonAsserts.add(mapper.<Map<String, Object>>readValue(line, MaxwellIntegrationTest.MAP_STRING_OBJECT_REF));
			} else {
				inputSQL.add(line);
			}
		}
		reader.close();

	    runJSONTest(inputSQL, jsonAsserts);
	}
}
