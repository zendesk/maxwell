package com.zendesk.maxwell.core.schema.ddl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.core.MaxwellTestJSON;
import com.zendesk.maxwell.core.MaxwellTestJSON.SQLAndJSON;
import com.zendesk.maxwell.core.MaxwellTestWithIsolatedServer;
import com.zendesk.maxwell.core.schema.Schema;
import com.zendesk.maxwell.core.schema.SchemaCapturer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by ben on 1/29/16.
 */
public class DDLSerializationTest extends MaxwellTestWithIsolatedServer {
	private List<SchemaChange> parse(String sql) {
		return SchemaChange.parse("default_db", sql);
	}

	private void TestDDLSerialization(String testFile) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Schema schema = new SchemaCapturer(server.getConnection(), buildContext().getCaseSensitivity()).capture();

		SQLAndJSON testResources = maxwellTestJSON.parseJSONTestFile(testFile);

		ArrayList<ResolvedSchemaChange> schemaChanges = new ArrayList<>();
		ArrayList<Map<String, Object>> schemaChangesAsJSON = new ArrayList<>();

		/* parse SQL into a list of ResolveSchemaChange objects */
		for ( String sql : testResources.inputSQL ) {
			for ( SchemaChange change : parse(sql) ) {
				ResolvedSchemaChange resolved = change.resolve(schema);
				if ( resolved != null ) {
					schemaChanges.add(resolved);
					resolved.apply(schema);
				}
			}
		}

		/* now serialize those objects into JSON and use the assertions in the text file. */
		for ( ResolvedSchemaChange change : schemaChanges ) {
			String json = mapper.writeValueAsString(change);
			Map<String, Object> m = mapper.readValue(json.getBytes(), MaxwellTestJSON.MAP_STRING_OBJECT_REF);

			/* test deserialization */
			ResolvedSchemaChange deserializedSchemaChange = mapper.readValue(json, ResolvedSchemaChange.class);
			String deserializedJSON = mapper.writeValueAsString(deserializedSchemaChange);
			Map<String, Object> m2 = mapper.readValue(deserializedJSON.getBytes(), MaxwellTestJSON.MAP_STRING_OBJECT_REF);

			assertThat(m2, is(m));

			schemaChangesAsJSON.add(m);
		}

		maxwellTestJSON.assertJSON(schemaChangesAsJSON, testResources.jsonAsserts);
	}

	@Test
	public void TestCreateDatabaseSerialization() throws Exception {
		TestDDLSerialization("serialization/create_database");
	}

	@Test
	public void TestCreateTableSerialization() throws Exception {
		if ( server.getVersion().atLeast(server.VERSION_5_6) )
			TestDDLSerialization("serialization/create_table");
		else
			TestDDLSerialization("serialization/create_table_55");
	}

	@Test
	public void TestAlterTableSerialization() throws Exception {
		TestDDLSerialization("serialization/alter_table");
	}

	@Test
	public void TestDropTableSerialization() throws Exception {
		TestDDLSerialization("serialization/drop_table");
	}
}
