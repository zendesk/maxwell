package com.zendesk.maxwell.schema.columndef;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;

import java.util.Arrays;

/**
 * Created by ben on 12/30/15.
 */
public class GeometryColumnDef extends ColumnDef {
	private GeometryColumnDef(String name, String type, short pos) {
		super(name, type, pos);
	}

	public static GeometryColumnDef create(String name, String type, short pos) {
		GeometryColumnDef temp = new GeometryColumnDef(name, type, pos);
		return (GeometryColumnDef) INTERNER.intern(temp);
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
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
			throw new ColumnDefCastException(this, value);
		}

		return geometry.toText();
	}

	@Override
	public String toSQL(Object value) {
		return null;
	}
}
