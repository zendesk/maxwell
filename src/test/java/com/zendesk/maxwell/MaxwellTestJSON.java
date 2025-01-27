package com.zendesk.maxwell;

import java.util.*;
import java.io.*;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.RowEncrypt;
import com.zendesk.maxwell.row.RowMap;
import org.apache.commons.lang3.StringUtils;

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

	public static Map<String, Object> parseEncryptedJSON(Map<String,Object> json, String secretKey) throws Exception {
		Map<String, String> encrypted = (Map)json.get("encrypted");
		if (encrypted == null) {
			return null;
		}
		String init_vector = encrypted.get("iv");
		String plaintext = RowEncrypt.decrypt(encrypted.get("bytes").toString(), secretKey, init_vector);
		return parseJSON(plaintext);
	}

	public static void assertJSON(List<Map<String, Object>> jsonOutput, List<Map<String, Object>> jsonAsserts) {
		ArrayList<Map<String, Object>> missing = new ArrayList<>();

		for ( Map m : jsonAsserts ) {
			if ( !jsonOutput.contains(m) )
				missing.add(m);
		}

		if ( missing.size() > 0 ) {
			String msg = "Did not find:\n" +
					StringUtils.join(missing.iterator(), "\n") +
					"\n\n in:\n" +
					StringUtils.join(jsonOutput.iterator(), "\n");
			assertThat(msg, false, is(true));
		}
	}

	private static List<RowMap> runJSONTest(MysqlIsolatedServer server, List<String> sql, List<Map<String, Object>> expectedJSON,
									Consumer<MaxwellConfig> configLambda) throws Exception {
		List<Map<String, Object>> eventJSON = new ArrayList<>();

		final MaxwellConfig captureConfig = new MaxwellConfig();
		if ( configLambda != null )
			configLambda.accept(captureConfig);

		MaxwellOutputConfig outputConfig = captureConfig.outputConfig;

		List<RowMap> rows = MaxwellTestSupport.getRowsWithReplicator(server, sql.toArray(new String[sql.size()]), null, configLambda);

		for ( RowMap r : rows ) {
			String s;
			if ( outputConfig == null ) {
				s = r.toJSON();
			} else {
				s = r.toJSON(outputConfig);
			}

			Map<String, Object> outputMap = parseJSON(s);

			outputMap.remove("ts");
			outputMap.remove("xid");
			outputMap.remove("xoffset");
			outputMap.remove("commit");

			eventJSON.add(outputMap);
		}
		assertJSON(eventJSON, expectedJSON);

		return rows;
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
					line = line.replaceAll("^\\s*->\\s*", "");
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

	protected static List<RowMap> runJSONTestFile(MysqlIsolatedServer server, String fname, Consumer<MaxwellConfig> configLambda) throws Exception {
		String dir = MaxwellTestSupport.getSQLDir();
		SQLAndJSON testResources = parseJSONTestFile(new File(dir, fname).toString());

		return runJSONTest(server, testResources.inputSQL, testResources.jsonAsserts, configLambda);
	}
}
