/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.zendesk.maxwell.replication.vitess;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenient wrapper that wraps the raw bytes value and converts it to Java
 * value.
 */
public class VitessColumnValue {
	private static final Logger LOGGER = LoggerFactory.getLogger(VitessColumnValue.class);

	private final byte[] value;

	public VitessColumnValue(byte[] value) {
		this.value = value;
	}

	public byte[] getRawValue() {
		return value;
	}

	public boolean isNull() {
		return value == null;
	}

	public byte[] asBytes() {
		return value;
	}

	/**
	 * Convert raw bytes value to string using UTF-8 encoding.
	 *
	 * This is *enforced* for VARCHAR and CHAR types, and is *required* for other
	 * non-bytes types (numeric,
	 * timestamp, etc.). For bytes (BLOB, BINARY, etc.) types, the asBytes() should
	 * be used.
	 *
	 * @return the UTF-8 string
	 */
	public String asString() {
		return new String(value, StandardCharsets.UTF_8);
	}

	public Integer asInteger() {
		return Integer.valueOf(asString());
	}

	public Short asShort() {
		return Short.valueOf(asString());
	}

	public Long asLong() {
		return Long.valueOf(asString());
	}

	public Float asFloat() {
		return Float.valueOf(asString());
	}

	public Double asDouble() {
		return Double.valueOf(asString());
	}

	public Object asDefault(VitessType vitessType) {
		LOGGER.warn("processing unknown column type {} as string", vitessType);
		return asString();
	}
}
