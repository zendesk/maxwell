package com.zendesk.maxwell.schema.columndef;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

abstract public class EnumeratedColumnDef extends ColumnDef  {
	@JsonProperty("enum-values")
	private final List<String> enumValues;

	public EnumeratedColumnDef(String name, String type, short pos, String [] enumValues, boolean nullable) {
		super(name, type, pos, nullable);
		ImmutableList.Builder<String> builder = ImmutableList.builderWithExpectedSize(enumValues.length);
		for (String enumValue : enumValues) {
			builder.add(enumValue.intern());
		}
		this.enumValues = builder.build();
	}

	public List<String> getEnumValues() {
		return enumValues;
	}

	@Override
	public boolean equals(Object o) {
		if (o.getClass() == getClass()) {
			EnumeratedColumnDef other = (EnumeratedColumnDef)o;
			return super.equals(o)
					&& Objects.equals(enumValues, other.enumValues);
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = super.hashCode();
		return 31 * hash + Objects.hash(enumValues);
	}
}
