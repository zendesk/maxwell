package com.zendesk.maxwell.schema.columndef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import org.apache.commons.lang3.StringUtils;

public class SetColumnDef extends EnumeratedColumnDef {
	public SetColumnDef(String name, String type, short pos, String[] enumValues, boolean nullable) {
		super(name, type, pos, enumValues, nullable);
	}

	public static SetColumnDef create(String name, String type, short pos, String[] enumValues, boolean nullable) {
		SetColumnDef temp = new SetColumnDef(name, type, pos, enumValues, nullable);
		return (SetColumnDef) INTERNER.intern(temp);
	}

	@Override
	public String toSQL(Object value) throws ColumnDefCastException {
		return "'" + StringUtils.join(asList(value), "'") + "'";
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		return asList(value);
	}

	private ArrayList<String> asList(Object value) throws ColumnDefCastException {
		if ( value instanceof String ) {
			return new ArrayList<>(Arrays.asList((( String ) value).split(",")));
		} else if ( value instanceof Long ) {
			ArrayList<String> values = new ArrayList<>();
			long v = (Long) value;
			List<String> enumValues = getEnumValues();
			for (int i = 0; i < enumValues.size(); i++) {
				if (((v >> i) & 1) == 1) {
					values.add(enumValues.get(i));
				}
			}
			return values;
		} else {
			throw new ColumnDefCastException(this, value);
		}
	}
}
