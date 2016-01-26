package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Created by ben on 12/30/15.
 */
public class GeometryColumnDef extends ColumnDef {
	public GeometryColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_GEOMETRY;
	}

	@Override
	public Object asJSON(Object value) {
		Geometry g = (Geometry) value;
		return g.toText();
	}

	@Override
	public String toSQL(Object value) {
		return null;
	}
}
