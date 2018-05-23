package com.zendesk.maxwell.schema.columndef;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

import java.util.Arrays;

/**
 * Created by ben on 12/30/15.
 */
public class GeometryColumnDef extends ColumnDef {
	public GeometryColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	@Override
	public Object asJSON(Object value) {
		Geometry geometry = null;
		if ( value instanceof Geometry ) {
			geometry = (Geometry) value;
		} else if ( value instanceof byte[] ) {
			byte []bytes = (byte[]) value;

			// mysql sprinkles 4 mystery bytes on top of the GIS data.
			bytes = Arrays.copyOfRange(bytes, 4, bytes.length);
			final WKBReader reader = new WKBReader();

			try {
				geometry = reader.read(bytes);
			} catch ( ParseException e ) {
				throw new RuntimeException("Could not parse geometry: " + e);
			}

		} else {
			throw new RuntimeException("Could not parse geometry column value: " + value);
		}

		return geometry.toText();
	}

	@Override
	public String toSQL(Object value) {
		return null;
	}
}
