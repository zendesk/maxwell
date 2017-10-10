package com.zendesk.maxwell.schema;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Testing the regex from method extractEnumValues()
 * 
 * @author frederik.mortensen
 *
 */
public class SchemaCapturerTest {

	@Test
	public void testExtractEnumValues() throws Exception {
		String expandedType = "enum('a')";
		String[] result = SchemaCapturer.extractEnumValues(expandedType);
		assertEquals(1, result.length);
		assertEquals("a", result[0]);

		expandedType = "enum('a','b','c','d')";
		result = SchemaCapturer.extractEnumValues(expandedType);
		assertEquals(4, result.length);
		assertEquals("a", result[0]);
		assertEquals("b", result[1]);
		assertEquals("c", result[2]);
		assertEquals("d", result[3]);

		expandedType = "enum('','b','c','d')";
		result = SchemaCapturer.extractEnumValues(expandedType);
		assertEquals(4, result.length);
		assertEquals("", result[0]);
		assertEquals("b", result[1]);
		assertEquals("c", result[2]);
		assertEquals("d", result[3]);
		
		expandedType = "enum('','b\'b','c')";
		result = SchemaCapturer.extractEnumValues(expandedType);
		assertEquals(3, result.length);
		assertEquals("", result[0]);
		assertEquals("b'b", result[1]);
	}

}