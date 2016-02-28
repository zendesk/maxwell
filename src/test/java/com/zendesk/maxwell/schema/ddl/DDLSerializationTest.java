package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.zendesk.maxwell.AbstractIntegrationTest;
import com.zendesk.maxwell.MaxwellIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Created by ben on 1/29/16.
 */

public class DDLSerializationTest extends AbstractIntegrationTest {
	private List<SchemaChange> parse(String sql) {
		return SchemaChange.parse("default_db", sql);
	}

	private void TestDDLSerialization(String testFile) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Schema schema = new SchemaCapturer(server.getConnection(), buildContext().getCaseSensitivity()).capture();

		SQLAndJSON testResources = parseJSONTestFile(testFile);

		ArrayList<ResolvedSchemaChange> schemaChanges = new ArrayList<>();
		ArrayList<Map<String, Object>> schemaChangesAsJSON = new ArrayList<>();

		for ( String sql : testResources.inputSQL ) {
			for ( SchemaChange change : parse(sql) ) {
				ResolvedSchemaChange resolved = change.resolve(schema);
				if ( resolved != null ) {
					schemaChanges.add(resolved);
					schema = resolved.apply(schema);
				}
			}
		}

		for ( ResolvedSchemaChange change : schemaChanges ) {
			String json = mapper.writeValueAsString(change);
			Map<String, Object> m = mapper.readValue(json.getBytes(), MaxwellIntegrationTest.MAP_STRING_OBJECT_REF);

			/* test deserialization */
			ResolvedSchemaChange deserializedSchemaChange = mapper.readValue(json, ResolvedSchemaChange.class);
			String deserializedJSON = mapper.writeValueAsString(deserializedSchemaChange);
			Map<String, Object> m2 = mapper.readValue(deserializedJSON.getBytes(), MaxwellIntegrationTest.MAP_STRING_OBJECT_REF);

			assertThat(m2, is(m));

			schemaChangesAsJSON.add(m);
		}

		assertJSON(schemaChangesAsJSON, testResources.jsonAsserts);
	}

	@Test
	public void TestCreateDatabaseSerialization() throws Exception {
		TestDDLSerialization(getSQLDir() + "/serialization/create_database");
	}

	@Test
	public void TestCreateTableSerialization() throws Exception {
		TestDDLSerialization(getSQLDir() + "/serialization/create_table");
	}

	@Test
	public void TestAlterTableSerialization() throws Exception {
		TestDDLSerialization(getSQLDir() + "/serialization/alter_table");
	}

	@Test
	public void TestDropTableSerialization() throws Exception {
		TestDDLSerialization(getSQLDir() + "/serialization/drop_table");
	}
}
