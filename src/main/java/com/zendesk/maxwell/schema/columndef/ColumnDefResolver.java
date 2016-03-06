package com.zendesk.maxwell.schema.columndef;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;

public class ColumnDefResolver extends TypeIdResolverBase {
	@Override
	public String idFromValue(Object value) {
		return ((ColumnDef) value).getType();
	}

	@Override
	public String idFromValueAndType(Object value, Class<?> suggestedType) {
		return idFromValue(value);
	}

	@Override
	public JsonTypeInfo.Id getMechanism() {
		return JsonTypeInfo.Id.CUSTOM;
	}

	@Override
	public JavaType typeFromId(DatabindContext context, String id) {
		ObjectMapper m = new ObjectMapper();

		switch(id) {
			case "tinyint":
			case "smallint":
			case "mediumint":
			case "int":
				return m.constructType(IntColumnDef.class);
			case "bigint":
				return m.constructType(BigIntColumnDef.class);
			case "tinytext":
			case "text":
			case "mediumtext":
			case "longtext":
			case "varchar":
			case "char":
				return m.constructType(StringColumnDef.class);
			case "tinyblob":
			case "blob":
			case "mediumblob":
			case "longblob":
			case "binary":
			case "varbinary":
				return m.constructType(StringColumnDef.class);
			case "geometry":
			case "geometrycollection":
			case "linestring":
			case "multilinestring":
			case "multipoint":
			case "multipolygon":
			case "polygon":
			case "point":
				return m.constructType(GeometryColumnDef.class);
			case "float":
			case "double":
				return m.constructType(FloatColumnDef.class);
			case "decimal":
				return m.constructType(DecimalColumnDef.class);
			case "date":
				return m.constructType(DateColumnDef.class);
			case "datetime":
			case "timestamp":
				return m.constructType(DateTimeColumnDef.class);
			case "year":
				return m.constructType(YearColumnDef.class);
			case "time":
				return m.constructType(TimeColumnDef.class);
			case "enum":
				return m.constructType(EnumColumnDef.class);
			case "set":
				return m.constructType(SetColumnDef.class);
			case "bit":
				return m.constructType(BitColumnDef.class);
			default:
				throw new IllegalArgumentException("unsupported column type " + id);
		}
	}
}
