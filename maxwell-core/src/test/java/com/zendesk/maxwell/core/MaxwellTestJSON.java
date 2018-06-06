package com.zendesk.maxwell.core;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.core.config.MaxwellFilter;
import com.zendesk.maxwell.core.config.MaxwellOutputConfig;
import com.zendesk.maxwell.core.row.RowEncrypt;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.support.MaxwellTestSupport;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Service
public class MaxwellTestJSON {
	/* methods around running JSON test files */
	public static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_REF = new TypeReference<Map<String, Object>>() {};

	@Autowired
	private MaxwellTestSupport maxwellTestSupport;

	public Map<String, Object> parseJSON(String json) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		return mapper.readValue(json, MAP_STRING_OBJECT_REF);
	}

	public Map<String, Object> parseEncryptedJSON(Map<String,Object> json, String secretKey) throws Exception {
		Map<String, String> encrypted = (Map)json.get("encrypted");
		if (encrypted == null) {
			return null;
		}
		String init_vector = encrypted.get("iv");
		String plaintext = RowEncrypt.decrypt(encrypted.get("bytes").toString(), secretKey, init_vector);
		return parseJSON(plaintext);
	}

	public void assertJSON(List<Map<String, Object>> jsonOutput, List<Map<String, Object>> jsonAsserts) {
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

	private void runJSONTest(MysqlIsolatedServer server, List<String> sql, List<Map<String, Object>> expectedJSON,
							 MaxwellFilter filter, MaxwellOutputConfig outputConfig) throws Exception {
		List<Map<String, Object>> eventJSON = new ArrayList<>();
		List<RowMap> rows = maxwellTestSupport.getRowsWithReplicator(server, filter, sql.toArray(new String[sql.size()]), null);

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

	}

	public static class SQLAndJSON {
		public ArrayList<Map<String, Object>> jsonAsserts;
		public ArrayList<String> inputSQL;

		protected SQLAndJSON() {
			this.jsonAsserts = new ArrayList<>();
			this.inputSQL = new ArrayList<>();
		}
	}

	private static final String JSON_PATTERN = "^\\s*\\->\\s*\\{.*";
	public SQLAndJSON parseJSONTestFile(String fname) throws Exception {
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

	protected void runJSONTestFile(MysqlIsolatedServer server, String dir, String fname, MaxwellFilter filter,
										  MaxwellOutputConfig outputConfig) throws Exception {
		SQLAndJSON testResources = parseJSONTestFile(new File(dir, fname).toString());
		runJSONTest(server, testResources.inputSQL, testResources.jsonAsserts, filter, outputConfig);
	}

	protected void runJSONTestFile(MysqlIsolatedServer server, String fname, MaxwellFilter filter,
										  MaxwellOutputConfig outputConfig) throws Exception {
		runJSONTestFile(server, MaxwellTestSupport.getSQLDir(), fname, filter, outputConfig);
	}
}
